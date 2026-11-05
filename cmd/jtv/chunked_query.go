package main

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"regexp"
	"strings"
)

// Chunked execution.
//
// The DuckDB Go driver materializes a whole result in memory before handing
// over the first row, so exporting 100 million rows held 4.5GB. A query
// that only selects and filters rows is therefore run over consecutive row
// id ranges instead,
//
//	from input  ->  from (select * from input where jtv_rowid >= A and jtv_rowid < B) as input
//
// one range at a time. DuckDB skips the blocks outside each range, so the
// ranges together cost about one scan, each holds at most chunkRows rows,
// and rows come out in file order as before. Only a query whose physical
// plan consists of scans, filters, projections and the key-lookup join is
// split; anything else (aggregates, sorts, limits, deduplication, windows,
// count(*) answered from statistics, operators unknown to jtv) needs all
// rows at once and runs unchanged. DuckDB's EXPLAIN decides, not a parse of
// the SQL text.

// chunkRows is the number of rows per range; a variable for tests.
var chunkRows = 1 << 20

// chunkableOperators are the physical plan operators whose output for a
// range of rows does not depend on rows outside it.
var chunkableOperators = map[string]bool{
	"PROJECTION": true,
	"FILTER":     true,
	"TABLE_SCAN": true,
	"SEQ_SCAN":   true,
	"HASH_JOIN":  true, // the semi join of a key-table lookup
}

// planOperator matches an operator box title in DuckDB 2.0 EXPLAIN output,
// such as "╭─ Table Scan ───╮". Any other format finds no operators, and the
// query then runs whole.
var planOperator = regexp.MustCompile(`╭─\s*([A-Za-z_][A-Za-z_ ]*?)\s*─`)

// chunkPlan returns the statement template for chunked execution, with %d
// placeholders for the range, or ok=false when the query must run whole.
func (d *Dataset) chunkPlan(ctx context.Context, query string) (string, bool) {
	if d.stream != nil || d.RowCount <= chunkRows {
		return "", false
	}
	positions := topLevelWordPositions(query)
	fromAt, ok := positions["from"]
	if !ok {
		return "", false
	}
	tableAt := skipSpaces(query, fromAt+len("from"))
	table, tableEnd := identAt(query, tableAt)
	if !strings.EqualFold(table, "input") {
		return "", false
	}
	// Exactly one reference to input, the top-level one.
	if strings.Count(strings.ToLower(query), "input") != 1 {
		return "", false
	}
	if d.keyLookupIsSmall(ctx, query) {
		return "", false
	}
	if !d.planIsChunkable(ctx, query) {
		return "", false
	}
	ranged := "(select * from input where " + rowOrderColumn + " >= %d and " + rowOrderColumn + " < %d) as input"
	escaped := strings.ReplaceAll(query[:tableAt], "%", "%%") + ranged + strings.ReplaceAll(query[tableEnd:], "%", "%%")
	return escaped, true
}

func (d *Dataset) planIsChunkable(ctx context.Context, query string) bool {
	rows, err := d.db.QueryContext(ctx, "explain "+query)
	if err != nil {
		return false
	}
	defer rows.Close()
	var plan strings.Builder
	cols, _ := rows.Columns()
	values := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range values {
		dest[i] = &values[i]
	}
	for rows.Next() {
		if rows.Scan(dest...) != nil {
			return false
		}
		for _, v := range values {
			plan.WriteString(v.String)
		}
	}
	if rows.Err() != nil {
		return false
	}
	operators := planOperator.FindAllStringSubmatch(plan.String(), -1)
	if len(operators) == 0 {
		return false // unknown plan format: run whole
	}
	for _, match := range operators {
		name := strings.ToUpper(strings.Join(strings.Fields(match[1]), "_"))
		if !chunkableOperators[name] {
			return false
		}
	}
	return true
}

// queryChunks runs a chunk plan over the whole table and passes the rows on
// in order. The first chunk's columns are reported once.
func (d *Dataset) queryChunks(ctx context.Context, template string, onColumns func([]string) error, onRow func([]string, []any) error) error {
	sentColumns := false
	for start := 0; start < d.RowCount; start += chunkRows {
		end := start + chunkRows
		if end >= d.RowCount {
			end = math.MaxInt64 // the last range takes any rows past the count
		}
		chunk := fmt.Sprintf(template, start, end)
		if ordered, ok := orderFilteredRows(chunk); ok {
			chunk = ordered
		}
		rows, err := d.queryContext(ctx, chunk)
		if err != nil {
			return err
		}
		if err := d.forwardRows(rows, !sentColumns, onColumns, onRow); err != nil {
			return err
		}
		sentColumns = true
	}
	return nil
}

// forwardRows scans rows into the callbacks and closes them.
func (d *Dataset) forwardRows(rows *sql.Rows, sendColumns bool, onColumns func([]string) error, onRow func([]string, []any) error) error {
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	if sendColumns {
		if err := onColumns(normalizeResultColumns(cols)); err != nil {
			return err
		}
	}
	dest := make([]any, len(cols))
	for rows.Next() {
		values := make([]any, len(cols))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		if err := onRow(stringifyRow(values), values); err != nil {
			return err
		}
	}
	return rows.Err()
}

// keyLookupIsSmall reports whether the query selects rows through a key
// table lookup (see addKeyLookup) that matches at most chunkRows rows. Such
// a result fits in memory, and splitting it would only repeat the lookup
// for every range. Counting in the sorted key table reads just the blocks
// holding the matches.
func (d *Dataset) keyLookupIsSmall(ctx context.Context, query string) bool {
	const marker = "(select r from jtv_key_"
	start := strings.Index(query, marker)
	if start < 0 {
		return false
	}
	end := matchingParen(query, start)
	if end < 0 {
		return false
	}
	var matches int
	if err := d.db.QueryRowContext(ctx, "select count(*) from "+query[start:end+1]+" as jtv_lookup").Scan(&matches); err != nil {
		return false
	}
	return matches <= chunkRows
}

// matchingParen returns the index of the parenthesis closing the one at
// open, skipping quoted text, or -1.
func matchingParen(s string, open int) int {
	depth := 0
	for i := open; i < len(s); {
		switch ch := s[i]; ch {
		case '\'', '"', '`':
			i = skipQuoted(s, i, ch)
			continue
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
		i++
	}
	return -1
}
