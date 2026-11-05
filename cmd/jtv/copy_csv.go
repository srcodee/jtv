package main

import (
	"database/sql"
	"strings"
)

// Fast CSV export.
//
// Writing a large -o file.csv through Go formats every value in Go: 83
// seconds for 100 million rows. DuckDB's COPY writes CSV in parallel C++
// instead. Its output is only used where it is byte for byte what the Go
// writer produces, which a side-by-side comparison of both showed to hold
// for integer, boolean and text columns, with two differences to handle:
//
//   - an empty string is written as "" by DuckDB and as nothing by Go; it
//     is turned into NULL, which both write as nothing;
//   - Go quotes text starting with white space and the value \. while
//     DuckDB does not; results holding such text use the Go writer.
//
// Other column types (DOUBLE prints 2.0 where Go prints 2, dates, lists...)
// always use the Go writer.

// csvCopyDisabled turns the fast path off (tests compare against it).
var csvCopyDisabled bool

var csvCopyTypes = map[string]bool{
	"BIGINT": true, "INTEGER": true, "SMALLINT": true, "TINYINT": true, "HUGEINT": true,
	"UBIGINT": true, "UINTEGER": true, "USMALLINT": true, "UTINYINT": true, "UHUGEINT": true,
	"BOOLEAN": true, "VARCHAR": true,
}

// tryCopyCSV writes the result of query to path with DuckDB's COPY when the
// output is identical to the Go writer's. handled is false when the Go
// writer must be used.
func tryCopyCSV(ds *Dataset, query, path string) (handled bool, err error) {
	if csvCopyDisabled || ds.stream != nil {
		return false, nil
	}
	sqlText := ds.rewrite(ds.applyQueryDefaults(query))
	if strings.Contains(sqlText, "(select r from jtv_key_") {
		// Key lookups keep file order through an explicit ORDER BY, as in
		// QueryEach; plain scans keep it through DuckDB's insertion order.
		if ordered, ok := orderFilteredRows(sqlText); ok {
			sqlText = ordered
		}
	}
	names, types, ok := describeQuery(ds.db, sqlText)
	if !ok {
		return false, nil
	}
	seen := map[string]bool{}
	var selects, textChecks []string
	for i, name := range names {
		if !csvCopyTypes[types[i]] || seen[strings.ToLower(name)] {
			return false, nil
		}
		seen[strings.ToLower(name)] = true
		c := quoteIdent(name)
		if types[i] == "VARCHAR" {
			selects = append(selects, "nullif("+c+", '') as "+c)
			textChecks = append(textChecks, "coalesce(bool_or(regexp_matches("+c+`, '^[\s\x{0B}\x{85}\p{Z}]') or `+c+` = '\.'), false)`)
		} else {
			selects = append(selects, c)
		}
	}
	if len(names) == 0 {
		return false, nil
	}
	aliases := make([]string, len(names))
	for i, name := range names {
		aliases[i] = quoteIdent(name)
	}
	source := "(" + sqlText + ") as jtv_out(" + strings.Join(aliases, ", ") + ")"
	if len(textChecks) > 0 {
		flags := make([]bool, len(textChecks))
		dest := make([]any, len(textChecks))
		for i := range flags {
			dest[i] = &flags[i]
		}
		if err := ds.db.QueryRow("select " + strings.Join(textChecks, ", ") + " from " + source).Scan(dest...); err != nil {
			return false, nil
		}
		for _, needsGoQuoting := range flags {
			if needsGoQuoting {
				return false, nil
			}
		}
	}
	copySQL := "copy (select " + strings.Join(selects, ", ") + " from " + source + ") to " +
		sqlString(path) + " (format csv, header true, delimiter ',', quote '\"', escape '\"', nullstr '')"
	if _, err := ds.db.Exec(copySQL); err != nil {
		return true, enrichQueryError(err, ds.Fields, query)
	}
	return true, nil
}

// describeQuery returns a query's result column names, as jtv shows them,
// and DuckDB types.
func describeQuery(db *sql.DB, query string) (names, types []string, ok bool) {
	rows, err := db.Query("describe " + query)
	if err != nil {
		return nil, nil, false
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	values := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range values {
		dest[i] = &values[i]
	}
	var raw []string
	for rows.Next() {
		if rows.Scan(dest...) != nil || len(values) < 2 {
			return nil, nil, false
		}
		raw = append(raw, values[0].String)
		types = append(types, strings.ToUpper(values[1].String))
	}
	if rows.Err() != nil {
		return nil, nil, false
	}
	return normalizeResultColumns(raw), types, true
}
