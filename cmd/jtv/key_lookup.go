package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Key tables.
//
// A cached CSV dataset keeps, for each high-cardinality column, a small table
// of (value, row id) sorted by value. DuckDB stores min/max values per block,
// so looking a value up in the sorted table reads one block instead of
// scanning every row. A condition such as "phone" = '628...' is replaced by
//
//	jtv_rowid in (select r from jtv_key_N where k = '628...')
//
// which selects exactly the same rows: the key table holds every non-null
// value of the column with its row id. DuckDB pushes the matching row ids
// into the scan of the data table as a filter, which skips all blocks but
// the ones holding those rows: finding one phone number among 10 million
// rows drops from about 90ms to 7ms. The original condition must go: kept
// next to the lookup, it makes DuckDB scan the data table first.

// maxKeyTables bounds the cache growth and build time.
const maxKeyTables = 4

// Key tables are built on demand. The cache builder only records which
// columns qualify (text or integers, mostly distinct, such as phone numbers
// or ids); the first query that looks values up in such a column starts a
// background key builder and is answered by a normal scan, and later
// queries attach the finished key file. Sorting 100 million values takes
// about a minute and doubles the cache size, so files nobody searches by
// value never pay for it.

// keyColumn is a column that qualifies for a key table.
type keyColumn struct {
	name    string
	ordinal int
}

// recordKeyCandidates stores the columns that qualify for key tables.
func recordKeyCandidates(db *sql.DB, headers []string) error {
	if _, err := db.Exec("create table jtv_key_candidates (ordinal integer, name varchar)"); err != nil {
		return err
	}
	types, err := tableColumnTypes(db, "jtv_data")
	if err != nil {
		return err
	}
	var candidates []int
	var exprs []string
	for i, header := range headers {
		if t := types[header]; t == "VARCHAR" || t == "BIGINT" {
			candidates = append(candidates, i)
			exprs = append(exprs, "approx_count_distinct("+quoteIdent(header)+")")
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	var rowCount int64
	if err := db.QueryRow("select n from jtv_row_count").Scan(&rowCount); err != nil {
		return err
	}
	distinct := make([]int64, len(candidates))
	dest := make([]any, len(candidates))
	for i := range distinct {
		dest[i] = &distinct[i]
	}
	if err := db.QueryRow("select " + strings.Join(exprs, ", ") + " from jtv_data").Scan(dest...); err != nil {
		return err
	}
	recorded := 0
	for i, ordinal := range candidates {
		if recorded == maxKeyTables || distinct[i]*2 < rowCount {
			continue
		}
		if _, err := db.Exec("insert into jtv_key_candidates values (?, ?)", ordinal, headers[ordinal]); err != nil {
			return err
		}
		recorded++
	}
	return nil
}

// keyFilePath is where the key table of a column of a cache is stored.
func keyFilePath(cacheFile string, ordinal int) string {
	return fmt.Sprintf("%s.key-%d.duckdb", strings.TrimSuffix(cacheFile, ".duckdb"), ordinal)
}

// loadKeyTables attaches the key files of a cached dataset. It returns the
// key table of each qualifying column by lower-cased name, empty when the
// key file has not been built yet, and the qualifying columns.
func loadKeyTables(db *sql.DB, cacheFile string) (map[string]string, map[string]keyColumn) {
	rows, err := db.Query("select ordinal, name from jtv_key_candidates")
	if err != nil {
		return nil, nil
	}
	var columns []keyColumn
	for rows.Next() {
		var column keyColumn
		if rows.Scan(&column.ordinal, &column.name) == nil {
			columns = append(columns, column)
		}
	}
	rows.Close()
	tables := map[string]string{}
	candidates := map[string]keyColumn{}
	for _, column := range columns {
		lower := strings.ToLower(column.name)
		candidates[lower] = column
		tables[lower] = attachKeyFile(db, cacheFile, column.ordinal)
	}
	return tables, candidates
}

// attachKeyFile attaches a column's key file if it exists and returns the
// table to query, or "".
func attachKeyFile(db *sql.DB, cacheFile string, ordinal int) string {
	path := keyFilePath(cacheFile, ordinal)
	if _, err := os.Stat(path); err != nil {
		return ""
	}
	schema := fmt.Sprintf("jtv_key_%d", ordinal)
	// Opening the same cache twice in one process shares the DuckDB
	// instance, so the key file may already be attached.
	var attachedPath sql.NullString
	if err := db.QueryRow("select path from duckdb_databases() where database_name = ?", schema).Scan(&attachedPath); err == nil {
		if attachedPath.String == path {
			return schema + ".keys"
		}
		return ""
	}
	if _, err := db.Exec("attach " + sqlString(path) + " as " + schema + " (read_only)"); err != nil {
		return ""
	}
	return schema + ".keys"
}

// buildKeyFile writes the sorted key table of one column of a cache into
// keyFile, working on disk with a memory limit and replacing keyFile only
// once complete.
func buildKeyFile(cacheFile, column, keyFile string) (err error) {
	dir := filepath.Dir(keyFile)
	workName, err := reserveTempPath(dir, ".key-work-*.duckdb")
	if err != nil {
		return err
	}
	spillDir, err := os.MkdirTemp(dir, ".spill-")
	if err != nil {
		return err
	}
	defer func() {
		os.RemoveAll(spillDir)
		removeDuckDBFiles(workName)
	}()
	c := quoteIdent(column)
	sortSQL := fmt.Sprintf("create table keys as select %s as k, rowid as r from jtv_source.jtv_data where %s is not null order by k", c, c)

	// With enough free memory the sort runs in memory and only the result
	// is written, several times faster than sorting on disk; otherwise it
	// works on disk within the builder's memory limit.
	info, err := os.Stat(cacheFile)
	if err != nil {
		return err
	}
	var statements []string
	var db *sql.DB
	if enoughMemoryToBuild(info.Size()) {
		db, err = openDuckDB()
		statements = []string{
			"attach " + sqlString(cacheFile) + " as jtv_source (read_only)",
			sortSQL,
			"detach jtv_source",
			"attach " + sqlString(workName) + " as jtv_key_out",
			"copy from database memory to jtv_key_out",
			"detach jtv_key_out",
		}
	} else {
		db, err = openDuckDBAt(workName)
		statements = []string{
			"set memory_limit = " + sqlString(buildMemoryLimit()),
			"set temp_directory = " + sqlString(spillDir),
			"attach " + sqlString(cacheFile) + " as jtv_source (read_only)",
			sortSQL,
			"detach jtv_source",
			"checkpoint",
		}
	}
	if err != nil {
		return err
	}
	defer db.Close()
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			return err
		}
	}
	if err := db.Close(); err != nil {
		return err
	}
	os.Remove(workName + ".wal")
	return os.Rename(workName, keyFile)
}

// requestKeyTable makes sure the key file of a qualifying column gets
// built: in the background normally, inline when background builds are off
// (tests), in which case it is attached for the next query right away.
func (d *Dataset) requestKeyTable(lowerName string) {
	column, ok := d.keyCandidates[lowerName]
	if !ok || d.cacheFile == "" {
		return
	}
	keyFile := keyFilePath(d.cacheFile, column.ordinal)
	// A builder started earlier may have finished since the cache was
	// opened (a long-running jtv serve, an interactive session).
	if table := attachKeyFile(d.db, d.cacheFile, column.ordinal); table != "" {
		d.keyTables[lowerName] = table
		return
	}
	if backgroundCacheBuild {
		// Start it once the current query is done, so the builder does not
		// compete with the scan the user is waiting for.
		d.afterQuery = append(d.afterQuery, func() { startKeyBuilder(d.cacheFile, column.name, keyFile) })
		return
	}
	if buildKeyFile(d.cacheFile, column.name, keyFile) == nil {
		d.keyTables[lowerName] = attachKeyFile(d.db, d.cacheFile, column.ordinal)
	}
}

var (
	keyLiteral = `(?:'(?:[^']|'')*'|[+-]?[0-9]+(?:\.[0-9]+)?)`
	keyEquals  = regexp.MustCompile(`^("(?:[^"]|"")+")\s*(?:==|=)\s*(` + keyLiteral + `)$`)
	keyIn      = regexp.MustCompile(`(?i)^("(?:[^"]|"")+")\s+in\s*\(\s*(` + keyLiteral + `(?:\s*,\s*` + keyLiteral + `)*)\s*\)$`)
	keyLike    = regexp.MustCompile(`(?i)^("(?:[^"]|"")+")\s+(i?like)\s+'((?:[^'%_]|'')+)%'$`)
)

// addKeyLookup replaces a condition of a query on the input view that tests
// a key column for equality, membership or a prefix with a key table
// lookup. When the column qualifies but its key table is not built yet, the
// query is returned unchanged with the column's lower-cased name in missing.
func addKeyLookup(query string, keyTables map[string]string) (rewritten, missing string) {
	if len(keyTables) == 0 {
		return query, ""
	}
	positions := topLevelWordPositions(query)
	whereAt, ok := positions["where"]
	fromAt, hasFrom := positions["from"]
	if !ok || !hasFrom || fromAt > whereAt {
		return query, ""
	}
	if table, _ := identAt(query, skipSpaces(query, fromAt+len("from"))); !strings.EqualFold(table, "input") {
		return query, ""
	}
	for _, word := range []string{"union", "intersect", "except"} {
		if _, ok := positions[word]; ok {
			return query, ""
		}
	}
	clauseStart := whereAt + len("where")
	clauseEnd := len(query)
	for _, word := range []string{"group", "order", "having", "window", "qualify", "limit", "offset"} {
		if pos, ok := positions[word]; ok && pos > whereAt {
			clauseEnd = min(clauseEnd, pos)
		}
	}
	if semicolon := strings.LastIndexByte(query, ';'); semicolon > whereAt && semicolon < clauseEnd {
		clauseEnd = semicolon
	}
	clause := query[clauseStart:clauseEnd]
	conjuncts, ok := splitTopLevelAnd(clause)
	if !ok {
		return query, ""
	}
	for i, conjunct := range conjuncts {
		condition, column, ok := keyCondition(strings.TrimSpace(conjunct), keyTables)
		if !ok {
			continue
		}
		if condition == "" {
			return query, column
		}
		{
			conjuncts[i] = " " + condition + " "
			rebuilt := strings.TrimRight(strings.Join(conjuncts, "and"), " \t\r\n")
			tail := query[clauseEnd:]
			if tail != "" && !strings.HasPrefix(tail, ";") {
				tail = " " + strings.TrimLeft(tail, " \t\r\n")
			}
			return query[:clauseStart] + rebuilt + tail, ""
		}
	}
	return query, ""
}

// keyCondition returns the key table lookup replacing conjunct. When the
// conjunct tests a qualifying column whose key table is missing, condition
// is empty and column names it.
func keyCondition(conjunct string, keyTables map[string]string) (condition, column string, ok bool) {
	table := func(ident string) (string, string, bool) {
		name := strings.ToLower(strings.ReplaceAll(ident[1:len(ident)-1], `""`, `"`))
		t, ok := keyTables[name]
		return t, name, ok
	}
	result := func(t, name, predicate string) (string, string, bool) {
		if t == "" {
			return "", name, true
		}
		return rowOrderColumn + " in (select r from " + t + " where " + predicate + ")", name, true
	}
	if m := keyEquals.FindStringSubmatch(conjunct); m != nil {
		if t, name, ok := table(m[1]); ok {
			return result(t, name, "k = "+m[2])
		}
	}
	if m := keyIn.FindStringSubmatch(conjunct); m != nil {
		if t, name, ok := table(m[1]); ok {
			return result(t, name, "k in ("+m[2]+")")
		}
	}
	if m := keyLike.FindStringSubmatch(conjunct); m != nil {
		t, name, ok := table(m[1])
		// A sorted range only serves ILIKE when case cannot matter.
		if ok && (strings.EqualFold(m[2], "like") || !strings.ContainsFunc(m[3], isLetter)) {
			return result(t, name, "k like '"+m[3]+"%'")
		}
	}
	return "", "", false
}

func isLetter(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r > 127
}

// splitTopLevelAnd splits a condition on AND outside quotes and parentheses.
// ok is false when the condition has a top-level OR, which would make an
// added AND condition change its meaning.
func splitTopLevelAnd(clause string) ([]string, bool) {
	var parts []string
	depth, start := 0, 0
	for i := 0; i < len(clause); {
		ch := clause[i]
		switch {
		case ch == '\'' || ch == '"' || ch == '`':
			i = skipQuoted(clause, i, ch)
			continue
		case ch == '(':
			depth++
		case ch == ')':
			depth = max(0, depth-1)
		case depth == 0 && isIdentStart(rune(ch)) && (i == 0 || !isIdentPart(rune(clause[i-1]))):
			word, end := identAt(clause, i)
			switch strings.ToLower(word) {
			case "or":
				return nil, false
			case "and":
				parts = append(parts, clause[start:i])
				start = end
			}
			i = end
			continue
		}
		i++
	}
	return append(parts, clause[start:]), true
}
