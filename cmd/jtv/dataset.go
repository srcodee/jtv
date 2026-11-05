package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type Dataset struct {
	Source       string
	Fields       []string
	FieldLabels  map[string]string
	FieldTypes   map[string]string
	FieldSamples map[string]string
	ArrayFields  map[string]bool
	RowCount     int

	db *sql.DB
	// textFields holds the lower-cased names of text columns, whose values
	// are compared as strings even when the query uses a numeric literal.
	textFields map[string]bool
	// stream is set when the database belongs to a stream session.
	stream *streamSession
	// keyTables maps lower-cased field names of a cached dataset to their
	// sorted key tables, "" while not built yet (see key_lookup.go).
	keyTables map[string]string
	// keyCandidates are the columns that qualify for key tables.
	keyCandidates map[string]keyColumn
	// cacheFile is the cache database a cached dataset was opened from.
	cacheFile string
	// afterQuery runs once the current query has finished.
	afterQuery []func()
	// csv marks datasets loaded from delimited text; headerless ones had no
	// header row and got generated column names.
	csv        bool
	headerless bool
	// columnNames maps original field names to the names shown instead
	// (see column_names.go); base keeps the metadata under original names.
	columnNames map[string]string
	base        *baseFields
	// namesPath is the local file whose saved column names apply.
	namesPath string
	// tempDB is a temporary on-disk database holding a file too big to
	// load in memory, deleted on Close.
	tempDB string
	// streamBatch marks a --stream batch: many input lines answered as one,
	// each still a single row, so no automatic de-duplication across them.
	streamBatch bool
}

// streamSession reuses one database for every line of --stream input:
// opening DuckDB costs about 30ms and creating tables a few more, against
// well under a millisecond to replace the rows of an unchanged table.
type streamSession struct {
	db *sql.DB
	// schema is the column definition of the current jtv_data table.
	schema string
	// statements caches prepared statements for the current schema.
	statements map[string]*sql.Stmt
}

func newStreamSession() (*streamSession, error) {
	db, err := openDuckDB()
	if err != nil {
		return nil, err
	}
	// Each line is one row; worker threads only add scheduling overhead.
	if _, err := db.Exec("set threads = 1"); err != nil {
		db.Close()
		return nil, err
	}
	return &streamSession{db: db}, nil
}

func (s *streamSession) prepare(query string) (*sql.Stmt, error) {
	if stmt, ok := s.statements[query]; ok {
		return stmt, nil
	}
	stmt, err := s.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	if s.statements == nil {
		s.statements = map[string]*sql.Stmt{}
	}
	s.statements[query] = stmt
	return stmt, nil
}

// resetStatements drops prepared statements, which DuckDB invalidates when
// the table they read is replaced.
func (s *streamSession) resetStatements() {
	for _, stmt := range s.statements {
		stmt.Close()
	}
	s.statements = nil
}

func (s *streamSession) Close() error {
	s.resetStatements()
	return s.db.Close()
}

type QueryResult struct {
	Columns []string
	Rows    [][]string
	Values  [][]any
}

func NewDataset(data []byte, source string) (*Dataset, error) {
	return NewDatasetWithDelimiter(data, source, "")
}

func NewDatasetWithDelimiter(data []byte, source, delimiter string) (*Dataset, error) {
	return NewDatasetFromInput(data, "", source, delimiter)
}

// NewDatasetFromInput loads JSON or delimited input. path, when set, is a
// local file holding exactly data; CSV input is then read from it directly.
func NewDatasetFromInput(data []byte, path, source, delimiter string) (*Dataset, error) {
	return newDataset(data, path, source, delimiter, nil)
}

// newDataset loads input into the stream session's database when stream is
// set and the input is JSON; otherwise the dataset gets a database of its own.
func newDataset(data []byte, path, source, delimiter string, stream *streamSession) (*Dataset, error) {
	rows, jsonErr := parseJSONRows(data)
	if jsonErr != nil {
		ds, csvErr := newCSVDataset(data, path, source, delimiter)
		if csvErr != nil {
			return nil, fmt.Errorf("%w; CSV fallback failed: %w", jsonErr, csvErr)
		}
		return ds, nil
	}
	if len(rows) == 0 {
		return nil, errors.New("input contains no rows")
	}

	flatRows := make([]map[string]any, 0, len(rows))
	fieldSet := map[string]struct{}{"raw": {}}
	arrayPaths := map[string]struct{}{}
	for _, row := range rows {
		raw, _ := json.Marshal(row)
		expanded := expandFlattenedRows("", row, arrayPaths)
		for _, flat := range expanded {
			flat["raw"] = string(raw)
			for field := range flat {
				fieldSet[field] = struct{}{}
			}
			flatRows = append(flatRows, flat)
		}
	}

	fields := sortedKeys(fieldSet)
	labels := buildFieldLabels(fields, arrayPaths)
	fieldTypes, fieldSamples := buildFieldExamples(fields, flatRows)
	arrayFields := buildArrayFields(fields, arrayPaths)
	var db *sql.DB
	if stream != nil {
		db = stream.db
	} else {
		var err error
		if db, err = openDuckDB(); err != nil {
			return nil, err
		}
	}

	ds := &Dataset{
		Source:       source,
		Fields:       fields,
		FieldLabels:  labels,
		FieldTypes:   fieldTypes,
		FieldSamples: fieldSamples,
		ArrayFields:  arrayFields,
		RowCount:     len(flatRows),
		db:           db,
		stream:       stream,
	}
	if err := ds.load(flatRows); err != nil {
		ds.Close()
		return nil, err
	}
	return ds, nil
}

// queryContext runs a query, through the stream session's prepared
// statements when there is one: --stream repeats the same query for every
// line, and preparing it once halves DuckDB's per-query cost.
func (d *Dataset) queryContext(ctx context.Context, query string) (*sql.Rows, error) {
	if d.stream == nil {
		return d.db.QueryContext(ctx, query)
	}
	stmt, err := d.stream.prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx)
}

func (d *Dataset) Close() error {
	if d.stream != nil {
		return nil // owned by the stream session
	}
	err := d.db.Close()
	removeLoadDB(d.tempDB)
	return err
}

func (d *Dataset) Query(ctx context.Context, query string) (*QueryResult, error) {
	result := &QueryResult{}
	err := d.QueryEach(ctx, query, func(columns []string) error {
		result.Columns = columns
		return nil
	}, func(row []string, values []any) error {
		result.Rows = append(result.Rows, row)
		result.Values = append(result.Values, values)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// QueryEach runs a query and hands over the columns, then each row as it is
// read, so callers can write results of any size without holding them all.
// values is owned by the callback.
func (d *Dataset) QueryEach(ctx context.Context, query string, onColumns func([]string) error, onRow func(row []string, values []any) error) error {
	defer d.runAfterQuery()
	query = d.applyQueryDefaults(query)
	rewritten := d.rewrite(query)
	if template, ok := d.chunkPlan(ctx, rewritten); ok {
		if err := d.queryChunks(ctx, template, onColumns, onRow); err != nil {
			return enrichQueryError(err, d.Fields, query)
		}
		return nil
	}
	var rows *sql.Rows
	var err error
	if ordered, ok := orderFilteredRows(rewritten); ok {
		// The explicit order is only a speed-up; if it does not fit the
		// query (e.g. an aggregate without GROUP BY), run the query as is.
		rows, err = d.queryContext(ctx, ordered)
	}
	if rows == nil {
		rows, err = d.queryContext(ctx, rewritten)
	}
	if err != nil {
		return enrichQueryError(err, d.Fields, query)
	}
	return d.forwardRows(rows, true, onColumns, onRow)
}

func (d *Dataset) runAfterQuery() {
	pending := d.afterQuery
	d.afterQuery = nil
	for _, f := range pending {
		f()
	}
}

func (d *Dataset) Count(ctx context.Context, query string) (int, error) {
	query = d.applyQueryDefaults(query)
	rewritten := strings.TrimSpace(d.rewrite(query))
	rewritten = strings.TrimSuffix(rewritten, ";")
	countSQL := fmt.Sprintf("select count(*) from (%s) as jtv_count", rewritten)

	var total int
	if err := d.db.QueryRowContext(ctx, countSQL).Scan(&total); err != nil {
		return 0, enrichQueryError(err, d.Fields, query)
	}
	return total, nil
}

// rewrite turns a jtv query into the SQL DuckDB runs.
func (d *Dataset) rewrite(query string) string {
	sql := rewriteQuery(query, d.Fields)
	sql = sqliteStringLiterals(sql, d.Fields)
	sql = castPatternOperands(sql, d.Fields, d.textFields)
	sql = quoteTextComparisons(sql, d.Fields, d.textFields)
	sql = orderGroupsByKey(sql)
	sql = orderTextNumerically(sql, d.textFields)
	sql, missing := addKeyLookup(sql, d.keyTables)
	if missing != "" {
		d.requestKeyTable(missing)
	}
	return sql
}

func enrichQueryError(err error, fields []string, query string) error {
	if err == nil {
		return nil
	}
	field := ""
	if match := missingColumnPattern.FindStringSubmatch(err.Error()); match != nil {
		field = strings.Trim(match[1], "`\"'")
	} else if match := missingTablePattern.FindStringSubmatch(err.Error()); match != nil {
		// DuckDB reads an unknown dotted field such as user.nmae as
		// table.column; recover the whole path from the query.
		path := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(match[1]) + `\.[A-Za-z0-9_.]+`)
		field = path.FindString(query)
	}
	field = strings.TrimSuffix(field, ";")
	if field == "" {
		return err
	}
	// DuckDB's binder message spans several lines with a caret diagram;
	// the short form matches what jtv printed with SQLite.
	if suggestion := closestField(field, fields); suggestion != "" {
		return fmt.Errorf("no such column: %s; did you mean %s?", field, suggestion)
	}
	return fmt.Errorf("no such column: %s; use ls or schema to list available fields", field)
}

// missingColumnPattern matches DuckDB's binder error for unknown columns.
var missingColumnPattern = regexp.MustCompile(`Referenced column ("[^"]+"|\S+) not found`)

// missingTablePattern matches DuckDB's binder error for unknown table names.
var missingTablePattern = regexp.MustCompile(`Referenced table "([^"]+)" not found`)

func closestField(target string, fields []string) string {
	bestField := ""
	bestDistance := 0
	target = strings.ToLower(target)
	for _, field := range fields {
		if field == "raw" {
			continue
		}
		candidate := strings.ToLower(field)
		if fieldBaseName(candidate) == target {
			return field
		}
		distance := levenshtein(target, candidate)
		limit := max(2, len(target)/3)
		if distance > limit {
			continue
		}
		if bestField == "" || distance < bestDistance {
			bestField = field
			bestDistance = distance
		}
	}
	return bestField
}

func fieldBaseName(field string) string {
	parts := strings.Split(field, ".")
	return parts[len(parts)-1]
}

func levenshtein(a, b string) int {
	if a == b {
		return 0
	}
	if a == "" {
		return len(b)
	}
	if b == "" {
		return len(a)
	}
	prev := make([]int, len(b)+1)
	curr := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		curr[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			curr[j] = min(min(prev[j]+1, curr[j-1]+1), prev[j-1]+cost)
		}
		prev, curr = curr, prev
	}
	return prev[len(b)]
}

func (d *Dataset) applyQueryDefaults(query string) string {
	// A single row has no duplicates; skipping the grouping matters for
	// --stream, which queries one row per line.
	if d.RowCount > 1 && !d.streamBatch && shouldDistinctScalarQuery(query, d.Fields, d.ArrayFields) {
		return addDistinctToSelect(query)
	}
	return query
}

func parseJSONRows(data []byte) ([]any, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, errors.New("empty input")
	}

	var value any
	if err := json.Unmarshal(trimmed, &value); err == nil {
		switch v := value.(type) {
		case []any:
			return v, nil
		case map[string]any:
			return []any{v}, nil
		default:
			return []any{v}, nil
		}
	}

	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	var rows []any
	for {
		var row any
		if err := decoder.Decode(&row); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("invalid JSON or NDJSON: %w", err)
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return nil, errors.New("invalid JSON or NDJSON")
	}
	return rows, nil
}

func csvDelimiter(data []byte, delimiter string) (rune, error) {
	delimiter = strings.TrimSpace(delimiter)
	switch strings.ToLower(delimiter) {
	case "", "auto":
		return detectCSVDelimiter(data), nil
	case "tab", "\\t", "t":
		return '\t', nil
	}
	runes := []rune(delimiter)
	if len(runes) != 1 {
		return 0, fmt.Errorf("delimiter must be one character, auto, or tab")
	}
	switch runes[0] {
	case ',', ';', '|', '\t':
		return runes[0], nil
	default:
		return 0, fmt.Errorf("unsupported delimiter %q; use comma, semicolon, pipe, or tab", delimiter)
	}
}

func detectCSVDelimiter(data []byte) rune {
	line := firstNonEmptyLine(data)
	candidates := []rune{',', ';', '|', '\t'}
	best := ','
	bestCount := -1
	for _, candidate := range candidates {
		count := strings.Count(line, string(candidate))
		if count > bestCount {
			best = candidate
			bestCount = count
		}
	}
	return best
}

func firstNonEmptyLine(data []byte) string {
	for len(data) > 0 {
		var line []byte
		line, data = cutLine(data)
		if line = bytes.TrimSpace(line); len(line) > 0 {
			return string(line)
		}
	}
	return ""
}

func parseScalar(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.EqualFold(value, "true") {
		return true
	}
	if strings.EqualFold(value, "false") {
		return false
	}
	if i, err := strconv.ParseInt(value, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		return f
	}
	return value
}

func expandFlattenedRows(prefix string, value any, arrayPaths map[string]struct{}) []map[string]any {
	switch v := value.(type) {
	case map[string]any:
		if len(v) == 0 && prefix != "" {
			return []map[string]any{{prefix: "{}"}}
		}
		rows := []map[string]any{{}}
		for key, child := range v {
			path := key
			if prefix != "" {
				path = prefix + "." + key
			}
			childRows := expandFlattenedRows(path, child, arrayPaths)
			rows = combineRows(rows, childRows)
		}
		return rows
	case []any:
		if prefix != "" {
			arrayPaths[prefix] = struct{}{}
		}
		if len(v) == 0 {
			if prefix == "" {
				return []map[string]any{{"value": "[]"}}
			}
			return []map[string]any{{prefix: "[]"}}
		}
		rows := make([]map[string]any, 0, len(v))
		for _, child := range v {
			rows = append(rows, expandFlattenedRows(prefix, child, arrayPaths)...)
		}
		return rows
	default:
		if prefix == "" {
			prefix = "value"
		}
		return []map[string]any{{prefix: v}}
	}
}

func combineRows(left, right []map[string]any) []map[string]any {
	if len(left) == 0 {
		return right
	}
	if len(right) == 0 {
		return left
	}
	combined := make([]map[string]any, 0, len(left)*len(right))
	for _, lrow := range left {
		for _, rrow := range right {
			row := make(map[string]any, len(lrow)+len(rrow))
			for key, value := range lrow {
				row[key] = value
			}
			for key, value := range rrow {
				row[key] = value
			}
			combined = append(combined, row)
		}
	}
	return combined
}

func buildFieldLabels(fields []string, arrayPaths map[string]struct{}) map[string]string {
	labels := make(map[string]string, len(fields))
	for _, field := range fields {
		labels[field] = labelField(field, arrayPaths)
	}
	return labels
}

func buildFieldExamples(fields []string, rows []map[string]any) (map[string]string, map[string]string) {
	types := make(map[string]string, len(fields))
	samples := make(map[string]string, len(fields))
	for _, row := range rows {
		for _, field := range fields {
			if field == "raw" {
				continue
			}
			if types[field] != "" && samples[field] != "" {
				continue
			}
			value, ok := row[field]
			if !ok || value == nil {
				continue
			}
			if types[field] == "" {
				types[field] = valueTypeName(value)
			}
			if samples[field] == "" {
				samples[field] = sampleValue(value)
			}
		}
	}
	for _, field := range fields {
		if field == "raw" {
			continue
		}
		if types[field] == "" {
			types[field] = "null"
		}
	}
	return types, samples
}

func valueTypeName(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case bool:
		return "bool"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "int"
	case float32, float64, json.Number:
		return "number"
	case string:
		return "string"
	default:
		return "json"
	}
}

func sampleValue(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return truncateDisplay(v, 60)
	case []byte:
		return truncateDisplay(string(v), 60)
	default:
		return truncateDisplay(fmt.Sprint(v), 60)
	}
}

func truncateDisplay(value string, limit int) string {
	value = strings.Join(strings.Fields(value), " ")
	if len(value) <= limit {
		return value
	}
	if limit <= 3 {
		return value[:limit]
	}
	return value[:limit-3] + "..."
}

func buildArrayFields(fields []string, arrayPaths map[string]struct{}) map[string]bool {
	out := make(map[string]bool, len(fields))
	for _, field := range fields {
		out[field] = fieldTouchesArray(field, arrayPaths)
	}
	return out
}

func fieldTouchesArray(field string, arrayPaths map[string]struct{}) bool {
	parts := strings.Split(field, ".")
	path := ""
	for i, part := range parts {
		if i == 0 {
			path = part
		} else {
			path += "." + part
		}
		if _, ok := arrayPaths[path]; ok {
			return true
		}
	}
	return false
}

func labelField(field string, arrayPaths map[string]struct{}) string {
	parts := strings.Split(field, ".")
	path := ""
	for i, part := range parts {
		if i == 0 {
			path = part
		} else {
			path += "." + part
		}
		if _, ok := arrayPaths[path]; ok {
			parts[i] = part + "[]"
		}
	}
	return strings.Join(parts, ".")
}

func sqliteValue(value any) any {
	switch v := value.(type) {
	case nil:
		return nil
	case string:
		return v
	case bool:
		if v {
			return 1
		}
		return 0
	case int:
		return v
	case int8:
		return v
	case int16:
		return v
	case int32:
		return v
	case int64:
		return v
	case uint:
		return v
	case uint8:
		return v
	case uint16:
		return v
	case uint32:
		return v
	case uint64:
		return v
	case float64:
		return v
	case float32:
		return v
	case json.Number:
		if i, err := strconv.ParseInt(string(v), 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(string(v), 64); err == nil {
			return f
		}
		return string(v)
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprint(v)
		}
		return string(raw)
	}
}

func numericValue(value any) (float64, error) {
	switch v := value.(type) {
	case nil:
		return 0, nil
	case int64:
		return float64(v), nil
	case float64:
		return v, nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case string:
		return parseFormattedNumber(v)
	case []byte:
		return parseFormattedNumber(string(v))
	default:
		return parseFormattedNumber(fmt.Sprint(v))
	}
}

func parseFormattedNumber(value string) (float64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}

	var b strings.Builder
	for _, r := range value {
		switch {
		case unicode.IsDigit(r):
			b.WriteRune(r)
		case r == '.' || r == ',' || r == '-' || r == '+':
			b.WriteRune(r)
		}
	}
	cleaned := normalizeNumberSeparators(trimNumberSeparators(b.String()))
	if cleaned == "" || cleaned == "-" || cleaned == "+" {
		return 0, formattedNumberError(value)
	}
	n, err := strconv.ParseFloat(cleaned, 64)
	if err != nil {
		return 0, formattedNumberError(value)
	}
	return n, nil
}

func formattedNumberError(value string) error {
	return fmt.Errorf("cannot convert %q to number; expected a value like 1000, 1.000, Rp 1.000, or 1.234,50", value)
}

func dateTimeValue(value any) (time.Time, error) {
	switch v := value.(type) {
	case nil:
		return time.Time{}, fmt.Errorf("cannot convert null to date")
	case time.Time:
		return v, nil
	case int64:
		return time.Unix(v, 0), nil
	case float64:
		return time.Unix(int64(v), 0), nil
	case []byte:
		return parseDateTimeString(string(v))
	default:
		return parseDateTimeString(fmt.Sprint(v))
	}
}

func parseDateTimeString(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("cannot convert empty string to date")
	}
	if n, err := strconv.ParseInt(value, 10, 64); err == nil {
		return time.Unix(n, 0), nil
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
		"02/01/2006 15:04:05",
		"02/01/2006",
		"01/02/2006 15:04:05",
		"01/02/2006",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, value); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot convert %q to date; expected ISO date/time, unix seconds, or common slash date", value)
}

func trimNumberSeparators(value string) string {
	value = strings.TrimSpace(value)
	sign := ""
	if strings.HasPrefix(value, "-") || strings.HasPrefix(value, "+") {
		sign = value[:1]
		value = value[1:]
	}
	value = strings.Trim(value, ".,")
	return sign + value
}

func normalizeNumberSeparators(value string) string {
	value = keepLeadingSign(value)
	lastDot := strings.LastIndex(value, ".")
	lastComma := strings.LastIndex(value, ",")
	if lastDot == -1 && lastComma == -1 {
		return value
	}
	if lastDot != -1 && lastComma != -1 {
		decimal := byte('.')
		thousands := byte(',')
		if lastComma > lastDot {
			decimal = ','
			thousands = '.'
		}
		value = strings.ReplaceAll(value, string(thousands), "")
		return replaceLastSeparator(value, decimal)
	}

	separator := byte('.')
	if lastComma != -1 {
		separator = ','
	}
	if looksLikeThousandsSeparated(value, separator) {
		return strings.ReplaceAll(value, string(separator), "")
	}
	return replaceLastSeparator(value, separator)
}

func keepLeadingSign(value string) string {
	var b strings.Builder
	signWritten := false
	for i, r := range value {
		if (r == '-' || r == '+') && i == 0 && !signWritten {
			b.WriteRune(r)
			signWritten = true
			continue
		}
		if unicode.IsDigit(r) || r == '.' || r == ',' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func looksLikeThousandsSeparated(value string, separator byte) bool {
	parts := strings.Split(value, string(separator))
	if len(parts) < 2 {
		return false
	}
	first := strings.TrimLeft(parts[0], "+-")
	if len(first) == 0 || len(first) > 3 {
		return false
	}
	for _, part := range parts[1:] {
		if len(part) != 3 {
			return false
		}
	}
	return true
}

func replaceLastSeparator(value string, separator byte) string {
	index := strings.LastIndex(value, string(separator))
	if index == -1 {
		return value
	}
	value = strings.ReplaceAll(value[:index], string(separator), "") + "." + strings.ReplaceAll(value[index+1:], string(separator), "")
	return value
}

func stringifyRow(values []any) []string {
	row := make([]string, len(values))
	for i, value := range values {
		row[i] = displayString(value)
	}
	return row
}

func normalizeResultColumns(cols []string) []string {
	out := make([]string, len(cols))
	for i, col := range cols {
		col = strings.Trim(col, `"`)
		col = strings.ReplaceAll(col, "count_star()", "count(*)") // DuckDB's name for count(*)
		for name, udf := range jtvFunctions {
			col = strings.ReplaceAll(col, udf+"(", name+"(")
		}
		out[i] = col
	}
	return out
}

func quoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func shouldDistinctScalarQuery(query string, fields []string, arrayFields map[string]bool) bool {
	selectList, ok := selectList(query)
	if !ok {
		return false
	}
	expressions := splitSelectExpressions(selectList)
	if len(expressions) == 0 {
		return false
	}

	fieldMap := make(map[string]string, len(fields))
	for _, field := range fields {
		fieldMap[strings.ToLower(field)] = field
	}
	for _, expr := range expressions {
		fieldExpr := stripAlias(expr)
		if fieldExpr == "*" || strings.ContainsAny(fieldExpr, "()+-/*") {
			return false
		}
		fieldExpr = strings.Trim(fieldExpr, "`\"")
		actualField, ok := fieldMap[strings.ToLower(fieldExpr)]
		if !ok {
			return false
		}
		if arrayFields[actualField] {
			return false
		}
	}
	return true
}

// addDistinctToSelect removes duplicate rows from a plain field query.
// DuckDB's DISTINCT does not keep row order, so when the query has no
// ordering or grouping of its own, duplicates are folded with GROUP BY ALL
// and rows are ordered by their first appearance in the input.
func addDistinctToSelect(query string) string {
	for _, keyword := range []string{"group", "order", "having", "union", "join", "distinct"} {
		if hasTopLevelKeyword(query, keyword) {
			return addPlainDistinct(query)
		}
	}
	trimmed := strings.TrimRightFunc(query, unicode.IsSpace)
	semicolon := ""
	if strings.HasSuffix(trimmed, ";") {
		trimmed = strings.TrimRightFunc(strings.TrimSuffix(trimmed, ";"), unicode.IsSpace)
		semicolon = ";"
	}
	insertAt := len(trimmed)
	for _, keyword := range []string{"limit", "offset"} {
		if pos := topLevelKeywordPosition(trimmed, keyword); pos != -1 && pos < insertAt {
			insertAt = pos
		}
	}
	head := strings.TrimRightFunc(trimmed[:insertAt], unicode.IsSpace)
	tail := trimmed[insertAt:]
	if tail != "" {
		tail = " " + tail
	}
	return head + " group by all order by min(" + rowOrderColumn + ")" + tail + semicolon
}

// rowOrderColumn is a hidden column of the input view holding each row's
// position in the input.
const rowOrderColumn = "jtv_rowid"

func addPlainDistinct(query string) string {
	trimmed := strings.TrimLeftFunc(query, unicode.IsSpace)
	if len(trimmed) < len("select") || !strings.EqualFold(trimmed[:len("select")], "select") {
		return query
	}
	prefixLen := len(query) - len(trimmed)
	return query[:prefixLen] + trimmed[:len("select")] + " distinct" + trimmed[len("select"):]
}

func selectList(query string) (string, bool) {
	trimmed := strings.TrimSpace(query)
	if strings.HasSuffix(trimmed, ";") {
		trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
	}
	if !strings.HasPrefix(strings.ToLower(trimmed), "select ") {
		return "", false
	}
	afterSelect := strings.TrimSpace(trimmed[len("select "):])
	if strings.HasPrefix(strings.ToLower(afterSelect), "distinct ") {
		return "", false
	}
	stop := findImplicitFromInsertPosition(afterSelect)
	if stop == -1 {
		stop = len(afterSelect)
	}
	list := strings.TrimSpace(afterSelect[:stop])
	if list == "" {
		return "", false
	}
	return list, true
}

func splitSelectExpressions(list string) []string {
	var expressions []string
	start := 0
	depth := 0
	for i := 0; i < len(list); i++ {
		switch list[i] {
		case '\'', '"', '`':
			i = skipQuoted(list, i, list[i]) - 1
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				expressions = append(expressions, strings.TrimSpace(list[start:i]))
				start = i + 1
			}
		}
	}
	expressions = append(expressions, strings.TrimSpace(list[start:]))
	return expressions
}

func stripAlias(expr string) string {
	parts := strings.Fields(expr)
	if len(parts) >= 3 && strings.EqualFold(parts[len(parts)-2], "as") {
		return strings.Join(parts[:len(parts)-2], " ")
	}
	if len(parts) == 2 {
		return parts[0]
	}
	return expr
}

func rewriteQuery(query string, fields []string) string {
	query = expandImplicitInput(strings.TrimSpace(query))
	query = expandSelectStar(query, fields)

	sorted := append([]string(nil), fields...)
	sort.Slice(sorted, func(i, j int) bool {
		return len(sorted[i]) > len(sorted[j])
	})

	var b strings.Builder
	for i := 0; i < len(query); {
		ch := query[i]
		if ch == '\'' || ch == '"' || ch == '`' {
			next := copyQuoted(&b, query, i, ch)
			i = next
			continue
		}
		if isIdentStart(rune(ch)) && (i == 0 || !isIdentPart(rune(query[i-1]))) {
			if replacement, end, ok := rewriteKeyword(query, i); ok {
				b.WriteString(replacement)
				i = end
				continue
			}
		}
		if isIdentStart(rune(ch)) {
			if field, ok := matchField(query, i, sorted); ok {
				b.WriteString(quoteIdent(field))
				i += len(field)
				continue
			}
		}
		b.WriteByte(ch)
		i++
	}
	return b.String()
}

// rewriteKeyword maps jtv function names to their registered UDFs, e.g.
// int(x) to jtv_int(x), and LIKE to ILIKE so matching stays
// case-insensitive as it was with SQLite.
func rewriteKeyword(query string, start int) (string, int, bool) {
	end := start
	for end < len(query) && isIdentPart(rune(query[end])) {
		end++
	}
	word := strings.ToLower(query[start:end])
	if word == "like" {
		return "ilike", end, true
	}
	if udf, ok := jtvFunctions[word]; ok {
		next := end
		for next < len(query) && (query[next] == ' ' || query[next] == '\t') {
			next++
		}
		if next < len(query) && query[next] == '(' {
			return udf, end, true
		}
	}
	return "", 0, false
}

func expandImplicitInput(query string) string {
	if hasTopLevelFrom(query) {
		return query
	}
	trimmed := strings.TrimRightFunc(query, unicode.IsSpace)
	semicolon := ""
	if strings.HasSuffix(trimmed, ";") {
		trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
		semicolon = ";"
	}
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(trimmed)), "select ") {
		return query
	}
	insertAt := findImplicitFromInsertPosition(trimmed)
	if insertAt == -1 {
		return trimmed + " from input" + semicolon
	}
	return strings.TrimSpace(trimmed[:insertAt]) + " from input " + strings.TrimSpace(trimmed[insertAt:]) + semicolon
}

func expandSelectStar(query string, fields []string) string {
	selectFields := make([]string, 0, len(fields))
	for _, field := range fields {
		if field == "raw" {
			continue
		}
		selectFields = append(selectFields, field)
	}
	if len(selectFields) == 0 {
		return query
	}
	quotedFields := make([]string, len(selectFields))
	for i, field := range selectFields {
		quotedFields[i] = quoteIdent(field)
	}
	replacement := strings.Join(quotedFields, ", ")

	var b strings.Builder
	for i := 0; i < len(query); {
		ch := query[i]
		if ch == '\'' || ch == '"' || ch == '`' {
			i = copyQuoted(&b, query, i, ch)
			continue
		}
		if ch == '*' && isSelectStar(query, i) {
			b.WriteString(replacement)
			i++
			continue
		}
		b.WriteByte(ch)
		i++
	}
	return b.String()
}

func hasTopLevelFrom(query string) bool {
	return hasTopLevelKeyword(query, "from")
}

func hasTopLevelLimit(query string) bool {
	return hasTopLevelKeyword(query, "limit")
}

func hasTopLevelKeyword(query, keyword string) bool {
	return topLevelKeywordPosition(query, keyword) != -1
}

func topLevelKeywordPosition(query, keyword string) int {
	for i := 0; i < len(query); {
		ch := query[i]
		if ch == '\'' || ch == '"' || ch == '`' {
			i = skipQuoted(query, i, ch)
			continue
		}
		if isIdentStart(rune(ch)) {
			end := i + 1
			for end < len(query) && isIdentPart(rune(query[end])) {
				end++
			}
			if strings.EqualFold(query[i:end], keyword) {
				return i
			}
			i = end
			continue
		}
		i++
	}
	return -1
}

func findImplicitFromInsertPosition(query string) int {
	keywords := []string{"where", "group", "order", "limit", "having"}
	for i := 0; i < len(query); {
		ch := query[i]
		if ch == '\'' || ch == '"' || ch == '`' {
			i = skipQuoted(query, i, ch)
			continue
		}
		if isIdentStart(rune(ch)) {
			end := i + 1
			for end < len(query) && isIdentPart(rune(query[end])) {
				end++
			}
			word := query[i:end]
			for _, keyword := range keywords {
				if strings.EqualFold(word, keyword) {
					return i
				}
			}
			i = end
			continue
		}
		i++
	}
	return -1
}

func isSelectStar(query string, star int) bool {
	before := strings.ToLower(strings.TrimSpace(query[:star]))
	if before == "select" || strings.HasSuffix(before, ",") {
		return true
	}
	return false
}

func skipQuoted(s string, start int, quote byte) int {
	for i := start + 1; i < len(s); i++ {
		if s[i] == quote {
			if i+1 < len(s) && s[i+1] == quote {
				i++
				continue
			}
			return i + 1
		}
		if s[i] == '\\' && i+1 < len(s) {
			i++
		}
	}
	return len(s)
}

func copyQuoted(b *strings.Builder, s string, start int, quote byte) int {
	b.WriteByte(quote)
	for i := start + 1; i < len(s); i++ {
		b.WriteByte(s[i])
		if s[i] == quote {
			if i+1 < len(s) && s[i+1] == quote {
				i++
				b.WriteByte(s[i])
				continue
			}
			return i + 1
		}
		if s[i] == '\\' && i+1 < len(s) {
			i++
			b.WriteByte(s[i])
		}
	}
	return len(s)
}

func matchField(s string, start int, fields []string) (string, bool) {
	for _, field := range fields {
		if field == "raw" {
			continue
		}
		if isSQLKeyword(field) {
			continue
		}
		end := start + len(field)
		if end > len(s) || !strings.EqualFold(s[start:end], field) {
			continue
		}
		if start > 0 && isIdentPart(rune(s[start-1])) {
			continue
		}
		if end < len(s) && isIdentPart(rune(s[end])) {
			continue
		}
		return field, true
	}
	return "", false
}

func isSQLKeyword(word string) bool {
	switch strings.ToLower(word) {
	case "select", "from", "where", "group", "by", "order", "limit", "having",
		"and", "or", "not", "like", "as", "is", "null", "in", "between",
		"asc", "desc", "distinct", "case", "when", "then", "else", "end":
		return true
	default:
		return false
	}
}

func isIdentStart(r rune) bool {
	return unicode.IsLetter(r) || r == '_'
}

func isIdentPart(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '.'
}
