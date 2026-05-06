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
	"sort"
	"strconv"
	"strings"
	"unicode"

	"modernc.org/sqlite"
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
}

type QueryResult struct {
	Columns []string
	Rows    [][]string
	Values  [][]any
}

func init() {
	registerNumericFunction("int", func(value float64) driver.Value {
		return int64(value)
	})
	registerNumericFunction("float", func(value float64) driver.Value {
		return value
	})
	registerNumericFunction("number", func(value float64) driver.Value {
		return value
	})
	registerNumericFunction("money", func(value float64) driver.Value {
		return value
	})
}

func registerNumericFunction(name string, convert func(float64) driver.Value) {
	sqlite.MustRegisterDeterministicScalarFunction(name, 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
		value, err := numericValue(args[0])
		if err != nil {
			return nil, err
		}
		return convert(value), nil
	})
}

func NewDataset(data []byte, source string) (*Dataset, error) {
	return NewDatasetWithDelimiter(data, source, "")
}

func NewDatasetWithDelimiter(data []byte, source, delimiter string) (*Dataset, error) {
	rows, err := parseRows(data, delimiter)
	if err != nil {
		return nil, err
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
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
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
	}
	if err := ds.load(flatRows); err != nil {
		db.Close()
		return nil, err
	}
	return ds, nil
}

func (d *Dataset) Close() error {
	return d.db.Close()
}

func (d *Dataset) Query(ctx context.Context, query string) (*QueryResult, error) {
	query = d.applyQueryDefaults(query)
	rewritten := rewriteQuery(query, d.Fields)
	rows, err := d.db.QueryContext(ctx, rewritten)
	if err != nil {
		return nil, enrichQueryError(err, d.Fields)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	result := &QueryResult{Columns: normalizeResultColumns(cols)}
	for rows.Next() {
		values := make([]any, len(cols))
		dest := make([]any, len(cols))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		result.Values = append(result.Values, cloneValues(values))
		result.Rows = append(result.Rows, stringifyRow(values))
	}
	return result, rows.Err()
}

func (d *Dataset) Count(ctx context.Context, query string) (int, error) {
	query = d.applyQueryDefaults(query)
	rewritten := strings.TrimSpace(rewriteQuery(query, d.Fields))
	rewritten = strings.TrimSuffix(rewritten, ";")
	countSQL := fmt.Sprintf("select count(*) from (%s) as jtv_count", rewritten)

	var total int
	if err := d.db.QueryRowContext(ctx, countSQL).Scan(&total); err != nil {
		return 0, enrichQueryError(err, d.Fields)
	}
	return total, nil
}

func enrichQueryError(err error, fields []string) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	const prefix = "no such column: "
	index := strings.Index(message, prefix)
	if index == -1 {
		return err
	}
	field := strings.TrimSpace(message[index+len(prefix):])
	if parts := strings.Fields(field); len(parts) > 0 {
		field = parts[0]
	}
	field = strings.Trim(field, "`\"'")
	field = strings.TrimSuffix(field, ";")
	if field == "" {
		return err
	}
	if suggestion := closestField(field, fields); suggestion != "" {
		return fmt.Errorf("%w; did you mean %s?", err, suggestion)
	}
	return fmt.Errorf("%w; use ls or schema to list available fields", err)
}

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

func (d *Dataset) load(rows []map[string]any) error {
	columns := make([]string, 0, len(d.Fields))
	for _, field := range d.Fields {
		columns = append(columns, quoteIdent(field))
	}
	createSQL := fmt.Sprintf("create table input (%s)", strings.Join(columns, ", "))
	if _, err := d.db.Exec(createSQL); err != nil {
		return err
	}

	placeholders := make([]string, len(d.Fields))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	insertSQL := fmt.Sprintf(
		"insert into input (%s) values (%s)",
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, row := range rows {
		args := make([]any, len(d.Fields))
		for i, field := range d.Fields {
			args[i] = sqliteValue(row[field])
		}
		if _, err := stmt.Exec(args...); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (d *Dataset) applyQueryDefaults(query string) string {
	if shouldDistinctScalarQuery(query, d.Fields, d.ArrayFields) {
		return addDistinctToSelect(query)
	}
	return query
}

func parseRows(data []byte, delimiter string) ([]any, error) {
	rows, jsonErr := parseJSONRows(data)
	if jsonErr == nil {
		return rows, nil
	}
	rows, csvErr := parseCSVRows(data, delimiter)
	if csvErr == nil {
		return rows, nil
	}
	return nil, fmt.Errorf("%w; CSV fallback failed: %w", jsonErr, csvErr)
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

func parseCSVRows(data []byte, delimiter string) ([]any, error) {
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
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) < 2 {
		return nil, errors.New("CSV input requires a header and at least one data row")
	}

	headers := make([]string, len(records[0]))
	seen := map[string]int{}
	for i, header := range records[0] {
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

	rows := make([]any, 0, len(records)-1)
	for _, record := range records[1:] {
		row := make(map[string]any, len(headers))
		for i, header := range headers {
			value := ""
			if i < len(record) {
				value = record[i]
			}
			row[header] = parseScalar(value)
		}
		rows = append(rows, row)
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
	line := firstNonEmptyLine(string(data))
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

func firstNonEmptyLine(data string) string {
	for _, line := range strings.Split(strings.ReplaceAll(data, "\r\n", "\n"), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			return line
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
		switch v := value.(type) {
		case nil:
			row[i] = ""
		case []byte:
			row[i] = string(v)
		default:
			row[i] = fmt.Sprint(v)
		}
	}
	return row
}

func cloneValues(values []any) []any {
	out := make([]any, len(values))
	copy(out, values)
	return out
}

func normalizeResultColumns(cols []string) []string {
	out := make([]string, len(cols))
	for i, col := range cols {
		out[i] = strings.Trim(col, `"`)
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

func addDistinctToSelect(query string) string {
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
				return true
			}
			i = end
			continue
		}
		i++
	}
	return false
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
