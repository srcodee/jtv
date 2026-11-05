package main

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// DuckDB backend.
//
// Datasets live in an in-memory DuckDB database. CSV files are read by
// DuckDB's own parallel CSV reader, JSON input is flattened in Go and
// streamed in with an appender. jtv's helper functions are registered as
// jtv_* UDFs and rewriteQuery maps the user-facing names onto them, so they
// never clash with DuckDB built-ins such as year() or month().

// openDuckDB opens an in-memory database.
func openDuckDB() (*sql.DB, error) {
	return openDuckDBAt("")
}

// openDuckDBAt opens a database, in memory for an empty dsn, limited to one
// pooled connection, which keeps UDF registration and temporary objects on a
// single session.
func openDuckDBAt(dsn string) (*sql.DB, error) {
	connector, err := duckdb.NewConnector(dsn, nil)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)

	conn, err := db.Conn(context.Background())
	if err != nil {
		db.Close()
		return nil, err
	}
	defer conn.Close()
	if err := registerJTVFunctions(conn); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// jtvFunctions maps the function names users write to the registered UDFs.
var jtvFunctions = map[string]string{
	"int":         "jtv_int",
	"float":       "jtv_float",
	"number":      "jtv_number",
	"money":       "jtv_money",
	"date":        "jtv_date",
	"year":        "jtv_year",
	"month":       "jtv_month",
	"regexp_like": "jtv_regexp_like",
}

type jtvUDF struct {
	inputs int
	result duckdb.Type
	fn     duckdb.RowExecutorFn
}

func (f *jtvUDF) Config() duckdb.ScalarFuncConfig {
	anyType, _ := duckdb.NewTypeInfo(duckdb.TYPE_ANY)
	resultType, _ := duckdb.NewTypeInfo(f.result)
	inputs := make([]duckdb.TypeInfo, f.inputs)
	for i := range inputs {
		inputs[i] = anyType
	}
	// jtv helpers decide themselves what NULL means (e.g. int(NULL) is 0).
	return duckdb.ScalarFuncConfig{
		InputTypeInfos:      inputs,
		ResultTypeInfo:      resultType,
		SpecialNullHandling: true,
	}
}

func (f *jtvUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{RowExecutor: f.fn}
}

func numericUDF(convert func(float64) any, result duckdb.Type) *jtvUDF {
	return &jtvUDF{inputs: 1, result: result, fn: func(args []driver.Value) (any, error) {
		value, err := numericValue(args[0])
		if err != nil {
			return nil, err
		}
		return convert(value), nil
	}}
}

func dateUDF(convert func(t time.Time) any, result duckdb.Type) *jtvUDF {
	return &jtvUDF{inputs: 1, result: result, fn: func(args []driver.Value) (any, error) {
		t, err := dateTimeValue(args[0])
		if err != nil {
			return nil, err
		}
		return convert(t), nil
	}}
}

func registerJTVFunctions(conn *sql.Conn) error {
	toFloat := func(v float64) any { return v }
	udfs := map[string]*jtvUDF{
		"jtv_int":    numericUDF(func(v float64) any { return int64(v) }, duckdb.TYPE_BIGINT),
		"jtv_float":  numericUDF(toFloat, duckdb.TYPE_DOUBLE),
		"jtv_number": numericUDF(toFloat, duckdb.TYPE_DOUBLE),
		"jtv_money":  numericUDF(toFloat, duckdb.TYPE_DOUBLE),
		"jtv_date": dateUDF(func(t time.Time) any {
			return t.Format("2006-01-02")
		}, duckdb.TYPE_VARCHAR),
		"jtv_year": dateUDF(func(t time.Time) any {
			return int64(t.Year())
		}, duckdb.TYPE_BIGINT),
		"jtv_month": dateUDF(func(t time.Time) any {
			return int64(t.Month())
		}, duckdb.TYPE_BIGINT),
		"jtv_regexp_like": {inputs: 2, result: duckdb.TYPE_BOOLEAN, fn: func(args []driver.Value) (any, error) {
			if args[0] == nil || args[1] == nil {
				return false, nil
			}
			return regexp.MatchString(fmt.Sprint(args[1]), fmt.Sprint(args[0]))
		}},
	}
	for name, udf := range udfs {
		if err := duckdb.RegisterScalarUDF(conn, name, udf); err != nil {
			return fmt.Errorf("register %s: %w", name, err)
		}
	}
	return nil
}

// newCSVDataset loads delimited text with DuckDB's CSV reader. When the
// input did not come from a local file it is spooled to a temporary file,
// because read_csv reads from paths.
func newCSVDataset(data []byte, path, source, delimiter string) (*Dataset, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, errors.New("empty input")
	}
	comma, err := csvDelimiter(trimmed, delimiter)
	if err != nil {
		return nil, err
	}
	reader := csv.NewReader(bytes.NewReader(trimmed))
	reader.Comma = comma
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true
	var sample [][]string
	for len(sample) < headerSampleRows {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if len(sample) >= 2 {
				break // a record cut off at the end of a peeked prefix
			}
			return nil, err
		}
		sample = append(sample, append([]string(nil), record...))
	}
	hasHeader := csvHasHeader(sample)
	if len(sample) == 0 || (hasHeader && len(sample) < 2) {
		return nil, errors.New("CSV input requires a header and at least one data row")
	}
	headers := generatedHeaders(sample)
	if hasHeader {
		headers = csvHeaders(sample[0])
	}
	headerless := !hasHeader

	var afterLoad func()
	if path != "" && fileStartsClean(data) {
		var ds *Dataset
		var ok bool
		if ds, ok, afterLoad = loadCSVCached(path, comma, headers, hasHeader, source); ok {
			ds.headerless = headerless
			if afterLoad != nil {
				ds.afterQuery = append(ds.afterQuery, afterLoad)
			}
			return ds, nil
		}
	}
	if path == "" || !fileStartsClean(data) {
		tmp, err := os.CreateTemp("", "jtv-*.csv")
		if err != nil {
			return nil, err
		}
		defer os.Remove(tmp.Name())
		_, writeErr := tmp.Write(trimmed)
		closeErr := tmp.Close()
		if err := errors.Join(writeErr, closeErr); err != nil {
			return nil, err
		}
		path = tmp.Name()
	}

	db, tempDB, err := openLoadDB(path)
	if err != nil {
		return nil, err
	}
	if err := buildCSVTables(db, path, comma, headers, hasHeader, false); err != nil {
		db.Close()
		removeLoadDB(tempDB)
		return nil, err
	}
	ds, err := csvDatasetFromDB(db, headers, source)
	if err != nil {
		db.Close()
		removeLoadDB(tempDB)
		return nil, err
	}
	ds.headerless = headerless
	ds.tempDB = tempDB
	if afterLoad != nil {
		afterLoad()
	}
	return ds, nil
}

// openLoadDB opens the database a file is loaded into: in memory when the
// system has room for it (about 2.5 times the file size), otherwise a
// temporary database on disk with a memory limit, so files larger than
// memory still load. tempDB names that file, removed with removeLoadDB.
func openLoadDB(path string) (db *sql.DB, tempDB string, err error) {
	if info, statErr := os.Stat(path); statErr != nil || enoughMemoryToBuild(info.Size()) {
		db, err = openDuckDB()
		return db, "", err
	}
	tempDB, err = reserveTempPath(os.TempDir(), "jtv-load-*.duckdb")
	if err != nil {
		return nil, "", err
	}
	db, err = openDuckDBAt(tempDB)
	if err == nil {
		_, err = db.Exec("set memory_limit = " + sqlString(buildMemoryLimit()))
	}
	if err != nil {
		if db != nil {
			db.Close()
		}
		removeLoadDB(tempDB)
		return nil, "", err
	}
	return db, tempDB, nil
}

// removeLoadDB deletes a temporary load database and its spill files.
func removeLoadDB(tempDB string) {
	if tempDB == "" {
		return
	}
	removeDuckDBFiles(tempDB)
	os.RemoveAll(tempDB + ".tmp")
}

// fileStartsClean reports whether the input has no leading blank space, so
// DuckDB reading the original file sees the same header line jtv parsed.
func fileStartsClean(data []byte) bool {
	return len(data) > 0 && !isSpaceByte(data[0])
}

func isSpaceByte(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}

func sqlString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// buildCSVTables loads the CSV file into jtv_data, creates the input view
// over it, and records field metadata in jtv_meta.
//
// DuckDB reads the file with the types its sniffer picks from a sample, and
// rows that do not fit those types are captured with store_rejects instead
// of failing the load. Each column with rejected values is widened
// (BIGINT to DOUBLE to text) and the file read again, so a clean file is
// read once and a file with a few odd values twice. Rows rejected for other
// reasons, such as extra columns, fall back to reading everything as text.
//
// materialize stores the final column types in the table, as a cache must.
// A first load, which is thrown away once the cache exists, converts the
// last columns in the input view instead: an ALTER of 10 million values
// costs 200ms, a cast in the view next to nothing.
func buildCSVTables(db *sql.DB, path string, comma rune, headers []string, hasHeader, materialize bool) error {
	casts, err := readTypedCSV(db, path, comma, headers, hasHeader, materialize)
	if err != nil {
		if !errors.Is(err, errCSVNeedsTextLoad) {
			return err
		}
		if _, err := db.Exec("drop table if exists jtv_data"); err != nil {
			return err
		}
		if err := readTextCSV(db, path, comma, headers, hasHeader); err != nil {
			return err
		}
		casts = nil
	}
	view := "create view input as select *, to_json(jtv_data) as raw, rowid as " + rowOrderColumn + " from jtv_data"
	if len(casts) > 0 {
		var selects, rawArgs []string
		for _, header := range headers {
			expr := quoteIdent(header)
			if target, ok := casts[header]; ok {
				expr = "cast(" + expr + " as " + target + ")"
			}
			selects = append(selects, expr+" as "+quoteIdent(header))
			rawArgs = append(rawArgs, sqlString(header), expr)
		}
		view = "create view input as select " + strings.Join(selects, ", ") +
			", json_object(" + strings.Join(rawArgs, ", ") + ") as raw, rowid as " + rowOrderColumn + " from jtv_data"
	}
	if _, err := db.Exec(view); err != nil {
		return err
	}
	return writeCSVMeta(db, headers)
}

var errCSVNeedsTextLoad = errors.New("csv rows rejected for reasons other than types")

func csvReadOptions(path string, comma rune, headers []string, hasHeader bool) string {
	names := make([]string, len(headers))
	for i, header := range headers {
		names[i] = sqlString(header)
	}
	return fmt.Sprintf("%s, delim=%s, header=%t, names=[%s], quote='\"', escape='\"', null_padding=true",
		sqlString(path), sqlString(string(comma)), hasHeader, strings.Join(names, ", "))
}

func readTypedCSV(db *sql.DB, path string, comma rune, headers []string, hasHeader, materialize bool) (map[string]string, error) {
	forced := map[string]string{}
	// BIGINT is deliberately not a candidate: DuckDB rounds '4.5' into a
	// BIGINT column instead of rejecting it, so a decimal that appears after
	// the sniffer's sample would be silently corrupted. DOUBLE rejects
	// nothing numeric, and integral DOUBLE columns become BIGINT below.
	base := csvReadOptions(path, comma, headers, hasHeader) +
		", auto_type_candidates=['BOOLEAN','DOUBLE','VARCHAR']" +
		", store_rejects=true, rejects_table='jtv_rejects', rejects_scan='jtv_reject_scans'"
	for attempt := 0; ; attempt++ {
		options := base
		if len(forced) > 0 {
			entries := make([]string, 0, len(forced))
			for name, typ := range forced {
				entries = append(entries, sqlString(name)+": "+sqlString(typ))
			}
			sort.Strings(entries)
			options += ", types={" + strings.Join(entries, ", ") + "}"
		}
		if _, err := db.Exec("drop table if exists jtv_data; drop table if exists temp.jtv_rejects; drop table if exists temp.jtv_reject_scans"); err != nil {
			return nil, err
		}
		if _, err := db.Exec("create table jtv_data as select * from read_csv(" + options + ")"); err != nil {
			return nil, err
		}
		rejected, err := rejectedColumns(db)
		if err != nil {
			return nil, err
		}
		if len(rejected) == 0 {
			break
		}
		if attempt >= 2 {
			return nil, errCSVNeedsTextLoad
		}
		// The file is read again anyway: tell that read the integer columns
		// found in this one, so they need no conversion afterwards.
		if err := forceIntegralColumns(db, headers, rejected, forced); err != nil {
			return nil, err
		}
		// A value that is neither a number nor a boolean makes the column
		// text; a decimal in a column read as integers makes it DOUBLE.
		for _, column := range rejected {
			if forced[column] == "BIGINT" {
				forced[column] = "DOUBLE"
			} else {
				forced[column] = "VARCHAR"
			}
		}
	}
	if _, err := db.Exec("drop table if exists temp.jtv_rejects; drop table if exists temp.jtv_reject_scans"); err != nil {
		return nil, err
	}
	return refineCSVTypes(db, headers, materialize)
}

// forceIntegralColumns marks the DOUBLE columns of the current table that
// hold only exact integers as BIGINT for the next read. Rejected rows are
// missing from the table; should they hold a decimal, that read rejects it
// and the column goes back to DOUBLE.
func forceIntegralColumns(db *sql.DB, headers, rejected []string, forced map[string]string) error {
	skip := map[string]bool{}
	for _, column := range rejected {
		skip[column] = true
	}
	types, err := tableColumnTypes(db, "jtv_data")
	if err != nil {
		return err
	}
	var columns, exprs []string
	for _, header := range headers {
		if types[header] == "DOUBLE" && !skip[header] && forced[header] == "" {
			c := quoteIdent(header)
			columns = append(columns, header)
			exprs = append(exprs, fmt.Sprintf("coalesce(bool_and(%[1]s = trunc(%[1]s)) and max(abs(%[1]s)) < %[2]d, false)", c, int64(maxExactDouble)))
		}
	}
	if len(exprs) == 0 {
		return nil
	}
	integral := make([]bool, len(exprs))
	dest := make([]any, len(exprs))
	for i := range integral {
		dest[i] = &integral[i]
	}
	if err := db.QueryRow("select " + strings.Join(exprs, ", ") + " from jtv_data").Scan(dest...); err != nil {
		return err
	}
	for i, column := range columns {
		if integral[i] {
			forced[column] = "BIGINT"
		}
	}
	return nil
}

// maxExactDouble is 2^53: beyond it a DOUBLE cannot hold every integer, so
// such columns are reloaded as text and converted exactly.
const maxExactDouble = 1 << 53

// refineCSVTypes finishes the typed load in one scan: integral DOUBLE
// columns become BIGINT, booleans become INTEGER 1/0 as SQLite stored them,
// and text values are trimmed as parseScalar trimmed them.
func refineCSVTypes(db *sql.DB, headers []string, materialize bool) (map[string]string, error) {
	types, err := tableColumnTypes(db, "jtv_data")
	if err != nil {
		return nil, err
	}
	casts := map[string]string{}
	type check struct {
		header string
		kind   string
	}
	var checks []check
	var exprs []string
	for _, header := range headers {
		c := quoteIdent(header)
		switch types[header] {
		case "DOUBLE":
			checks = append(checks, check{header, "integral"}, check{header, "max"})
			exprs = append(exprs,
				fmt.Sprintf("coalesce(bool_and(%[1]s = trunc(%[1]s)), false)", c),
				fmt.Sprintf("coalesce(max(abs(%[1]s)), 0)", c))
		case "VARCHAR":
			checks = append(checks, check{header, "untrimmed"})
			exprs = append(exprs, fmt.Sprintf("coalesce(bool_or(starts_with(%[1]s, ' ') or ends_with(%[1]s, ' ') or starts_with(%[1]s, chr(9)) or ends_with(%[1]s, chr(9))), false)", c))
		}
	}
	values := make([]any, len(exprs))
	if len(exprs) > 0 {
		dest := make([]any, len(exprs))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := db.QueryRow("select " + strings.Join(exprs, ", ") + " from jtv_data").Scan(dest...); err != nil {
			return nil, err
		}
	}

	var statements []string
	integral := map[string]bool{}
	for i, ch := range checks {
		c := quoteIdent(ch.header)
		switch ch.kind {
		case "integral":
			integral[ch.header], _ = values[i].(bool)
		case "max":
			largest, _ := values[i].(float64)
			if largest >= maxExactDouble {
				return nil, errCSVNeedsTextLoad
			}
			if integral[ch.header] {
				if materialize {
					statements = append(statements, fmt.Sprintf("alter table jtv_data alter %s set data type bigint", c))
				} else {
					casts[ch.header] = "bigint"
				}
			}
		case "untrimmed":
			if untrimmed, _ := values[i].(bool); untrimmed {
				statements = append(statements, fmt.Sprintf("update jtv_data set %[1]s = trim(%[1]s) where %[1]s <> trim(%[1]s)", c))
			}
		}
	}
	for _, header := range headers {
		if types[header] == "BOOLEAN" {
			// SQLite stored booleans as 1/0; keep that for queries such as active = 1.
			if materialize {
				statements = append(statements, fmt.Sprintf("alter table jtv_data alter %s set data type integer", quoteIdent(header)))
			} else {
				casts[header] = "integer"
			}
		}
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			return nil, err
		}
	}
	return casts, nil
}

// rejectedColumns returns the columns with values that did not fit their
// type, or errCSVNeedsTextLoad when rows were rejected for other reasons.
func rejectedColumns(db *sql.DB) ([]string, error) {
	rows, err := db.Query("select distinct error_type, column_name from temp.jtv_rejects")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var errorType string
		var column sql.NullString
		if err := rows.Scan(&errorType, &column); err != nil {
			return nil, err
		}
		if errorType != "CAST" || !column.Valid {
			return nil, errCSVNeedsTextLoad
		}
		columns = append(columns, column.String)
	}
	return columns, rows.Err()
}

// writeCSVMeta stores the field types, samples and row count, so opening a
// cached dataset reads one small table instead of scanning the data.
func writeCSVMeta(db *sql.DB, headers []string) error {
	// The input view has the final types, also where it converts columns.
	types, err := tableColumnTypes(db, "input")
	if err != nil {
		return err
	}
	samples, err := columnSamples(db, "input", headers)
	if err != nil {
		return err
	}
	if _, err := db.Exec("create table jtv_meta (ordinal integer, name varchar, type varchar, sample varchar)"); err != nil {
		return err
	}
	for i, header := range headers {
		if _, err := db.Exec("insert into jtv_meta values (?, ?, ?, ?)", i, header, types[header], samples[header]); err != nil {
			return err
		}
	}
	_, err = db.Exec("create table jtv_row_count as select count(*) as n from jtv_data")
	return err
}

// readTextCSV loads every value as text and then converts each column to the
// narrowest type that fits all its values.
func readTextCSV(db *sql.DB, path string, comma rune, headers []string, hasHeader bool) error {
	// Reading everything as text never fails on a stray value late in the
	// file, unlike DuckDB's sampled type detection.
	readSQL := "create temp table jtv_text as select * from read_csv(" +
		csvReadOptions(path, comma, headers, hasHeader) + ", all_varchar=true)"
	if _, err := db.Exec(readSQL); err != nil {
		return err
	}
	_, selects, err := csvColumnTypes(db, headers)
	if err != nil {
		return err
	}
	if _, err := db.Exec("create table jtv_data as select " + strings.Join(selects, ", ") + " from jtv_text"); err != nil {
		return err
	}
	_, err = db.Exec("drop table jtv_text")
	return err
}

// csvColumnTypes picks a type per column the way parseScalar types values:
// BIGINT when every value is an integer, DOUBLE when every value is a
// number, INTEGER 1/0 when every value is true/false, otherwise text. It
// returns the chosen types and the select expressions converting jtv_text.
//
// Each check is a parallel scan counting the values that do not fit, so the
// work is split in two: every column gets the cheap integer check, and only
// columns that are not all integers pay for the other checks.
func csvColumnTypes(db *sql.DB, headers []string) (map[string]string, []string, error) {
	notInt := make([]string, 0, len(headers)*2)
	for _, header := range headers {
		c := quoteIdent(header)
		notInt = append(notInt,
			// try_cast accepts '1.5' (rounded), '1e3', '0x10' and '1_000';
			// parseScalar treated none of those as integers. Surrounding
			// spaces are fine: the cast trims them.
			fmt.Sprintf("count(*) filter (where %[1]s is not null and (try_cast(%[1]s as bigint) is null"+
				" or contains(%[1]s, '.') or contains(%[1]s, 'e') or contains(%[1]s, 'E')"+
				" or contains(%[1]s, 'x') or contains(%[1]s, '_')))", c),
			fmt.Sprintf("count(%s)", c),
		)
	}
	firstPass, err := queryCounts(db, notInt)
	if err != nil {
		return nil, nil, err
	}

	var rest []string
	var restHeaders []string
	for i, header := range headers {
		if firstPass[i*2+1] > 0 && firstPass[i*2] > 0 {
			c := quoteIdent(header)
			rest = append(rest,
				fmt.Sprintf("count(*) filter (where %[1]s is not null and try_cast(%[1]s as double) is null)", c),
				fmt.Sprintf("count(*) filter (where %[1]s is not null and lower(trim(%[1]s)) not in ('true', 'false'))", c),
				fmt.Sprintf("count(*) filter (where %[1]s <> trim(%[1]s))", c),
			)
			restHeaders = append(restHeaders, header)
		}
	}
	secondPass := map[string][3]int64{}
	if len(rest) > 0 {
		values, err := queryCounts(db, rest)
		if err != nil {
			return nil, nil, err
		}
		for i, header := range restHeaders {
			secondPass[header] = [3]int64{values[i*3], values[i*3+1], values[i*3+2]}
		}
	}

	types := make(map[string]string, len(headers))
	selects := make([]string, len(headers))
	for i, header := range headers {
		c := quoteIdent(header)
		nonNull, notIntCount := firstPass[i*2+1], firstPass[i*2]
		second := secondPass[header]
		notDouble, notBool, untrimmed := second[0], second[1], second[2]
		value := c
		if untrimmed > 0 {
			value = "trim(" + c + ")"
		}
		switch {
		case nonNull == 0:
			types[header] = "VARCHAR"
			selects[i] = c
		case notIntCount == 0:
			types[header] = "BIGINT"
			selects[i] = fmt.Sprintf("cast(%s as bigint) as %s", c, c)
		case notDouble == 0:
			types[header] = "DOUBLE"
			selects[i] = fmt.Sprintf("cast(%s as double) as %s", c, c)
		case notBool == 0:
			// SQLite stored booleans as 1/0; keep that for queries such as active = 1.
			types[header] = "INTEGER"
			selects[i] = fmt.Sprintf("cast(lower(trim(%s)) = 'true' as integer) as %s", c, c)
		default:
			types[header] = "VARCHAR"
			selects[i] = value + " as " + c
		}
	}
	return types, selects, nil
}

func queryCounts(db *sql.DB, exprs []string) ([]int64, error) {
	values := make([]int64, len(exprs))
	dest := make([]any, len(exprs))
	for i := range values {
		dest[i] = &values[i]
	}
	err := db.QueryRow("select " + strings.Join(exprs, ", ") + " from jtv_text").Scan(dest...)
	return values, err
}

func tableColumnTypes(db *sql.DB, table string) (map[string]string, error) {
	rows, err := db.Query("select column_name, data_type from information_schema.columns where table_name = ?", table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	types := map[string]string{}
	for rows.Next() {
		var name, dataType string
		if err := rows.Scan(&name, &dataType); err != nil {
			return nil, err
		}
		types[name] = dataType
	}
	return types, rows.Err()
}

func jtvTypeName(duckType string) string {
	switch duckType {
	case "INTEGER":
		// The CSV loader stores true/false columns as INTEGER 1/0 and real
		// integers as BIGINT.
		return "bool"
	case "BIGINT", "SMALLINT", "TINYINT", "HUGEINT", "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT":
		return "int"
	case "DOUBLE", "FLOAT", "REAL":
		return "number"
	case "BOOLEAN":
		return "bool"
	case "VARCHAR":
		return "string"
	case "":
		return "null"
	default:
		if strings.HasPrefix(duckType, "DECIMAL") {
			return "number"
		}
		return "json"
	}
}

// columnSamples returns the first non-empty value of each column.
func columnSamples(db *sql.DB, table string, columns []string) (map[string]string, error) {
	selects := make([]string, len(columns))
	for i, column := range columns {
		selects[i] = fmt.Sprintf("first(%s) filter (where %s is not null and cast(%s as varchar) <> '')",
			quoteIdent(column), quoteIdent(column), quoteIdent(column))
	}
	values := make([]any, len(columns))
	dest := make([]any, len(columns))
	for i := range values {
		dest[i] = &values[i]
	}
	query := fmt.Sprintf("select %s from (select * from %s limit 100000)", strings.Join(selects, ", "), quoteIdent(table))
	if err := db.QueryRow(query).Scan(dest...); err != nil {
		return nil, err
	}
	samples := make(map[string]string, len(columns))
	for i, column := range columns {
		if values[i] != nil {
			samples[column] = sampleValue(displayString(values[i]))
		}
	}
	return samples, nil
}

// loadJSONTable creates the input table from flattened JSON rows. A column
// gets the narrowest type that holds all its values: BIGINT for integers and
// booleans (stored as 1/0), DOUBLE for numbers, otherwise text.
func (d *Dataset) load(rows []map[string]any) error {
	kinds := make([]string, len(d.Fields))
	for i, field := range d.Fields {
		kinds[i] = jsonColumnKind(field, rows)
	}
	d.textFields = map[string]bool{}
	for i, field := range d.Fields {
		if kinds[i] == "VARCHAR" {
			d.textFields[strings.ToLower(field)] = true
		}
	}
	columns := make([]string, len(d.Fields))
	for i, field := range d.Fields {
		columns[i] = quoteIdent(field) + " " + kinds[i]
	}
	schema := strings.Join(columns, ", ")
	switch {
	case d.stream != nil && d.stream.schema == schema:
		clear, err := d.stream.prepare("delete from jtv_data")
		if err != nil {
			return err
		}
		if _, err := clear.Exec(); err != nil {
			return err
		}
	default:
		if d.stream != nil {
			d.stream.schema = ""
			d.stream.resetStatements()
			if _, err := d.db.Exec("drop view if exists input; drop table if exists jtv_data"); err != nil {
				return err
			}
		}
		if _, err := d.db.Exec("create table jtv_data (" + schema + ")"); err != nil {
			return err
		}
		if _, err := d.db.Exec("create view input as select *, rowid as " + rowOrderColumn + " from jtv_data"); err != nil {
			return err
		}
		if d.stream != nil {
			d.stream.schema = schema
		}
	}

	conn, err := d.db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Raw(func(driverConn any) error {
		appender, err := duckdb.NewAppenderFromConn(driverConn.(driver.Conn), "", "jtv_data")
		if err != nil {
			return err
		}
		args := make([]driver.Value, len(d.Fields))
		for _, row := range rows {
			for i, field := range d.Fields {
				args[i] = jsonColumnValue(row[field], kinds[i])
			}
			if err := appender.AppendRow(args...); err != nil {
				appender.Close()
				return err
			}
		}
		return appender.Close()
	})
}

func jsonColumnKind(field string, rows []map[string]any) string {
	kind := ""
	for _, row := range rows {
		value, ok := row[field]
		if !ok || value == nil {
			continue
		}
		var k string
		switch v := value.(type) {
		case bool:
			k = "BIGINT"
		case int, int8, int16, int32, int64, uint8, uint16, uint32:
			k = "BIGINT"
		case float64:
			if v == math.Trunc(v) && math.Abs(v) < 1<<53 {
				k = "BIGINT"
			} else {
				k = "DOUBLE"
			}
		case float32:
			k = "DOUBLE"
		default:
			return "VARCHAR"
		}
		switch {
		case kind == "":
			kind = k
		case kind != k:
			kind = "DOUBLE" // BIGINT and DOUBLE mix
		}
	}
	if kind == "" {
		return "VARCHAR"
	}
	return kind
}

func jsonColumnValue(value any, kind string) driver.Value {
	if value == nil {
		return nil
	}
	switch kind {
	case "BIGINT":
		switch v := value.(type) {
		case bool:
			if v {
				return int64(1)
			}
			return int64(0)
		case float64:
			return int64(v)
		}
		n, _ := numericValue(sqliteValue(value))
		return int64(n)
	case "DOUBLE":
		switch v := value.(type) {
		case bool:
			if v {
				return float64(1)
			}
			return float64(0)
		case float64:
			return v
		}
		n, _ := numericValue(sqliteValue(value))
		return n
	default:
		switch v := sqliteValue(value).(type) {
		case string:
			return v
		default:
			return displayString(v)
		}
	}
}

func csvHeaders(record []string) []string {
	headers := make([]string, len(record))
	seen := map[string]int{}
	for i, header := range record {
		header = strings.TrimSpace(header)
		if header == "" {
			header = fmt.Sprintf("column_%d", i+1)
		}
		seen[header]++
		if seen[header] > 1 {
			header = fmt.Sprintf("%s_%d", header, seen[header])
		}
		headers[i] = header
	}
	return headers
}

// cutLine returns the next line without its line ending.
func cutLine(data []byte) (line, rest []byte) {
	idx := bytes.IndexByte(data, '\n')
	if idx < 0 {
		line, rest = data, nil
	} else {
		line, rest = data[:idx], data[idx+1:]
	}
	if n := len(line); n > 0 && line[n-1] == '\r' {
		line = line[:n-1]
	}
	return line, rest
}

// displayString formats a DuckDB result value for tables and CSV output.
func displayString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	case time.Time:
		if v.Hour() == 0 && v.Minute() == 0 && v.Second() == 0 && v.Nanosecond() == 0 {
			return v.Format("2006-01-02")
		}
		return v.Format("2006-01-02 15:04:05")
	case []any, map[string]any, duckdb.Map:
		raw, err := json.Marshal(v)
		if err == nil {
			return string(raw)
		}
		return fmt.Sprint(v)
	case fmt.Stringer:
		return v.String()
	default:
		return fmt.Sprint(v)
	}
}
