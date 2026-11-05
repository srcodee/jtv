package main

import "strings"

// quoteTextComparisons adapts comparisons to DuckDB's typed columns so they
// behave as they did with SQLite, which stored every value with its own type:
//
//   - A text column (mixed values, e.g. phone numbers plus a few words)
//     compared for equality with a number compares as text:
//     "phone" = 628123 becomes "phone" = '628123', also in IN lists.
//   - A text column compared with <, >, <= or >= against a number compares
//     numerically, skipping values that are not numbers:
//     "phone" > 5 becomes try_cast("phone" as double) > 5.
//   - A non-text column compared with an empty string tests for a missing
//     value, because empty CSV cells used to be ” and are now NULL:
//     "code" = ” becomes "code" is null.
func quoteTextComparisons(query string, fields []string, textFields map[string]bool) string {
	known := make(map[string]bool, len(fields))
	for _, field := range fields {
		known[strings.ToLower(field)] = true
	}
	var b strings.Builder
	for i := 0; i < len(query); {
		ch := query[i]
		switch ch {
		case '\'', '`':
			i = copyQuoted(&b, query, i, ch)
			continue
		case '"':
			end := skipQuoted(query, i, '"')
			ident := query[i:end]
			name := strings.ToLower(strings.ReplaceAll(query[i+1:max(i+1, end-1)], `""`, `"`))
			i = end
			switch {
			case textFields[name]:
				if rangeComparedWithNumber(query, i) {
					b.WriteString("try_cast(" + ident + " as double)")
					continue
				}
				b.WriteString(ident)
				i = quoteComparedLiterals(&b, query, i)
			case known[name]:
				b.WriteString(ident)
				i = emptyStringToNull(&b, query, i)
			default:
				b.WriteString(ident)
			}
			continue
		}
		b.WriteByte(ch)
		i++
	}
	return b.String()
}

// rangeComparedWithNumber reports whether <, >, <= or >= and a numeric
// literal follow position i.
func rangeComparedWithNumber(query string, i int) bool {
	j := skipSpaces(query, i)
	if j >= len(query) || (query[j] != '<' && query[j] != '>') {
		return false
	}
	op := 1
	if j+1 < len(query) {
		switch query[j : j+2] {
		case "<>":
			return false
		case "<=", ">=":
			op = 2
		}
	}
	_, ok := numericLiteralEnd(query, skipSpaces(query, j+op))
	return ok
}

// emptyStringToNull rewrites = ” and <> ” after a non-text column into
// IS NULL and IS NOT NULL. It returns the position after what it consumed.
func emptyStringToNull(b *strings.Builder, query string, i int) int {
	j := skipSpaces(query, i)
	for _, op := range []struct{ text, replacement string }{
		{"==", " is null"}, {"=", " is null"}, {"<>", " is not null"}, {"!=", " is not null"},
	} {
		if !strings.HasPrefix(query[j:], op.text) {
			continue
		}
		k := skipSpaces(query, j+len(op.text))
		if strings.HasPrefix(query[k:], "''") && (k+2 >= len(query) || query[k+2] != '\'') {
			b.WriteString(op.replacement)
			return k + 2
		}
		return i
	}
	return i
}

// quoteComparedLiterals copies the comparison following a text column and
// quotes its numeric operands. It returns the position after what it copied.
func quoteComparedLiterals(b *strings.Builder, query string, i int) int {
	j := skipSpaces(query, i)
	for _, op := range []string{"<>", "!=", "==", "="} {
		if strings.HasPrefix(query[j:], op) {
			k := skipSpaces(query, j+len(op))
			if end, ok := numericLiteralEnd(query, k); ok {
				b.WriteString(query[i:k])
				b.WriteString("'" + query[k:end] + "'")
				return end
			}
			return i
		}
	}

	k := j
	if word, end := identAt(query, k); strings.EqualFold(word, "not") {
		k = skipSpaces(query, end)
	}
	word, end := identAt(query, k)
	if !strings.EqualFold(word, "in") {
		return i
	}
	open := skipSpaces(query, end)
	if open >= len(query) || query[open] != '(' {
		return i
	}
	// Quote the list only when every item is a numeric literal.
	var items [][2]int
	pos := open + 1
	for {
		start := skipSpaces(query, pos)
		stop, ok := numericLiteralEnd(query, start)
		if !ok {
			return i
		}
		items = append(items, [2]int{start, stop})
		pos = skipSpaces(query, stop)
		if pos < len(query) && query[pos] == ',' {
			pos++
			continue
		}
		if pos < len(query) && query[pos] == ')' {
			break
		}
		return i
	}
	last := i
	for _, item := range items {
		b.WriteString(query[last:item[0]])
		b.WriteString("'" + query[item[0]:item[1]] + "'")
		last = item[1]
	}
	b.WriteString(query[last : pos+1])
	return pos + 1
}

func skipSpaces(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	return i
}

func identAt(s string, i int) (string, int) {
	end := i
	for end < len(s) && isIdentPart(rune(s[end])) {
		end++
	}
	return s[i:end], end
}

// numericLiteralEnd matches an optionally signed integer or decimal literal
// that is not followed by an identifier character.
func numericLiteralEnd(s string, i int) (int, bool) {
	j := i
	if j < len(s) && (s[j] == '-' || s[j] == '+') {
		j++
	}
	digits := j
	for j < len(s) && s[j] >= '0' && s[j] <= '9' {
		j++
	}
	if j == digits {
		return 0, false
	}
	if j < len(s) && s[j] == '.' {
		j++
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			j++
		}
	}
	if j < len(s) && (isIdentPart(rune(s[j])) || s[j] == '.') {
		return 0, false
	}
	return j, true
}

// sqliteStringLiterals turns double-quoted words that name no field into
// string literals, e.g. like "62000%" becomes like '62000%'.
//
// SQLite falls back to a string when a double-quoted identifier does not
// resolve, so queries written for jtv often quote values that way. DuckDB
// would report an unknown column instead. Names defined with AS, and words
// after AS, FROM, JOIN or a dot, stay identifiers.
func sqliteStringLiterals(query string, fields []string) string {
	known := make(map[string]bool, len(fields)+1)
	for _, field := range fields {
		known[strings.ToLower(field)] = true
	}
	for _, name := range []string{"input", "jtv_data", rowOrderColumn} {
		known[name] = true
	}
	for _, alias := range quotedAliases(query) {
		known[strings.ToLower(alias)] = true
	}

	var b strings.Builder
	prev := ""
	for i := 0; i < len(query); {
		ch := query[i]
		switch {
		case ch == '\'' || ch == '`':
			i = copyQuoted(&b, query, i, ch)
			prev = "literal"
		case ch == '"':
			end := skipQuoted(query, i, '"')
			name := strings.ReplaceAll(query[i+1:max(i+1, end-1)], `""`, `"`)
			keepIdentifier := known[strings.ToLower(name)] || prev == "as" || prev == "from" ||
				prev == "join" || prev == "." || (end < len(query) && query[end] == '.')
			if keepIdentifier {
				b.WriteString(query[i:end])
			} else {
				b.WriteString("'" + strings.ReplaceAll(name, "'", "''") + "'")
			}
			prev = "identifier"
			i = end
		case isIdentStart(rune(ch)):
			word, end := identAt(query, i)
			b.WriteString(word)
			prev = strings.ToLower(word)
			i = end
		default:
			b.WriteByte(ch)
			if !strings.ContainsRune(" \t\r\n", rune(ch)) {
				prev = string(ch)
			}
			i++
		}
	}
	return b.String()
}

// quotedAliases returns the names defined as AS "name".
func quotedAliases(query string) []string {
	var aliases []string
	lower := strings.ToLower(query)
	for i := 0; i < len(query); {
		switch ch := query[i]; {
		case ch == '\'' || ch == '"' || ch == '`':
			i = skipQuoted(query, i, ch)
		case isIdentStart(rune(ch)):
			word, end := identAt(lower, i)
			i = end
			if word != "as" {
				continue
			}
			start := skipSpaces(query, end)
			if start < len(query) && query[start] == '"' {
				stop := skipQuoted(query, start, '"')
				aliases = append(aliases, strings.ReplaceAll(query[start+1:max(start+1, stop-1)], `""`, `"`))
				i = stop
			}
		default:
			i++
		}
	}
	return aliases
}

// orderGroupsByKey adds ORDER BY with the grouping keys to a query that
// groups without ordering. SQLite returned groups sorted by key, which jtv's
// count FIELD shortcut and most group queries relied on; DuckDB returns them
// in arbitrary order.
func orderGroupsByKey(query string) string {
	positions := topLevelWordPositions(query)
	groupAt, ok := positions["group"]
	if !ok {
		return query
	}
	if _, ordered := positions["order"]; ordered {
		return query
	}
	byWord, byEnd := identAt(query, skipSpaces(query, groupAt+len("group")))
	if !strings.EqualFold(byWord, "by") {
		return query
	}
	listEnd := len(query)
	insertAt := len(query)
	for _, keyword := range []string{"having", "window", "qualify", "limit", "offset"} {
		if pos, ok := positions[keyword]; ok && pos > byEnd {
			listEnd = min(listEnd, pos)
			if keyword == "limit" || keyword == "offset" {
				insertAt = min(insertAt, pos)
			}
		}
	}
	if semicolon := strings.LastIndexByte(query, ';'); semicolon > byEnd && semicolon < listEnd {
		listEnd = semicolon
		insertAt = min(insertAt, semicolon)
	}
	keys := strings.TrimSpace(query[byEnd:listEnd])
	if keys == "" || strings.EqualFold(keys, "all") {
		return query
	}
	head := strings.TrimRight(query[:insertAt], " \t\r\n")
	tail := query[insertAt:]
	if tail != "" && !strings.HasPrefix(tail, ";") {
		tail = " " + tail
	}
	return head + " order by " + keys + tail
}

// topLevelWordPositions returns the first position of each lower-cased word
// outside quotes and parentheses.
func topLevelWordPositions(query string) map[string]int {
	positions := map[string]int{}
	depth := 0
	for i := 0; i < len(query); {
		ch := query[i]
		switch {
		case ch == '\'' || ch == '"' || ch == '`':
			i = skipQuoted(query, i, ch)
			continue
		case ch == '(':
			depth++
		case ch == ')':
			depth = max(0, depth-1)
		case isIdentStart(rune(ch)) && (i == 0 || !isIdentPart(rune(query[i-1]))):
			word, end := identAt(query, i)
			if depth == 0 {
				lower := strings.ToLower(word)
				if _, seen := positions[lower]; !seen {
					positions[lower] = i
				}
			}
			i = end
			continue
		}
		i++
	}
	return positions
}

// castPatternOperands casts non-text fields used with LIKE, ILIKE or GLOB to
// text, e.g. "code" like '6%' becomes cast("code" as varchar) like '6%'.
// SQLite matched patterns against the text form of any value; DuckDB only
// matches text.
func castPatternOperands(query string, fields []string, textFields map[string]bool) string {
	nonText := make(map[string]bool, len(fields))
	for _, field := range fields {
		if !textFields[strings.ToLower(field)] {
			nonText[strings.ToLower(field)] = true
		}
	}
	if len(nonText) == 0 {
		return query
	}
	var b strings.Builder
	for i := 0; i < len(query); {
		ch := query[i]
		switch ch {
		case '\'', '`':
			i = copyQuoted(&b, query, i, ch)
			continue
		case '"':
			end := skipQuoted(query, i, '"')
			name := strings.ReplaceAll(query[i+1:max(i+1, end-1)], `""`, `"`)
			if nonText[strings.ToLower(name)] && followedByPatternOperator(query, end) {
				b.WriteString("cast(" + query[i:end] + " as varchar)")
			} else {
				b.WriteString(query[i:end])
			}
			i = end
			continue
		}
		b.WriteByte(ch)
		i++
	}
	return b.String()
}

func followedByPatternOperator(query string, i int) bool {
	word, end := identAt(query, skipSpaces(query, i))
	if strings.EqualFold(word, "not") {
		word, _ = identAt(query, skipSpaces(query, end))
	}
	switch strings.ToLower(word) {
	case "like", "ilike", "glob":
		return true
	}
	return false
}

// orderTextNumerically sorts text columns in ORDER BY the way SQLite sorted
// mixed values: numbers first, in numeric order, then the other text. A
// plain item such as "phone" desc becomes
//
//	try_cast("phone" as double) is null desc, try_cast("phone" as double) desc, "phone" desc
//
// so phone numbers of different lengths still sort by value.
func orderTextNumerically(query string, textFields map[string]bool) string {
	positions := topLevelWordPositions(query)
	orderAt, ok := positions["order"]
	if !ok {
		return query
	}
	byWord, byEnd := identAt(query, skipSpaces(query, orderAt+len("order")))
	if !strings.EqualFold(byWord, "by") {
		return query
	}
	listEnd := len(query)
	for _, keyword := range []string{"limit", "offset"} {
		if pos, ok := positions[keyword]; ok && pos > byEnd {
			listEnd = min(listEnd, pos)
		}
	}
	if semicolon := strings.LastIndexByte(query, ';'); semicolon > byEnd && semicolon < listEnd {
		listEnd = semicolon
	}

	items := splitTopLevelCommas(query[byEnd:listEnd])
	changed := false
	for i, item := range items {
		if expanded, ok := numericOrderItem(strings.TrimSpace(item), textFields); ok {
			items[i] = " " + expanded
			changed = true
		}
	}
	if !changed {
		return query
	}
	tail := query[listEnd:]
	if tail != "" && !strings.HasPrefix(tail, ";") {
		tail = " " + strings.TrimLeft(tail, " \t\r\n")
	}
	return query[:byEnd] + strings.TrimRight(strings.Join(items, ","), " \t\r\n") + tail
}

func numericOrderItem(item string, textFields map[string]bool) (string, bool) {
	if !strings.HasPrefix(item, `"`) {
		return "", false
	}
	end := skipQuoted(item, 0, '"')
	name := strings.ReplaceAll(item[1:max(1, end-1)], `""`, `"`)
	if !textFields[strings.ToLower(name)] {
		return "", false
	}
	direction := strings.ToLower(strings.TrimSpace(item[end:]))
	if direction != "" && direction != "asc" && direction != "desc" {
		return "", false
	}
	suffix := ""
	if direction != "" {
		suffix = " " + direction
	}
	ident := item[:end]
	number := "try_cast(" + ident + " as double)"
	return number + " is null" + suffix + ", " + number + suffix + ", " + ident + suffix, true
}

// splitTopLevelCommas splits a list on commas outside quotes and parentheses.
func splitTopLevelCommas(list string) []string {
	var items []string
	depth, start := 0, 0
	for i := 0; i < len(list); {
		switch ch := list[i]; ch {
		case '\'', '"', '`':
			i = skipQuoted(list, i, ch)
			continue
		case '(':
			depth++
		case ')':
			depth = max(0, depth-1)
		case ',':
			if depth == 0 {
				items = append(items, list[start:i])
				start = i + 1
			}
		}
		i++
	}
	return append(items, list[start:])
}

// orderFilteredRows adds ORDER BY jtv_rowid to a filtered row query on the
// input view that has no ordering of its own.
//
// DuckDB keeps insertion order by default, but for a filtered scan that
// costs about six times more than sorting the matches by row id afterwards
// (0.5s against 0.08s on 10 million rows). The rows come back in file order
// either way. ok is false when the query does not qualify.
func orderFilteredRows(query string) (string, bool) {
	positions := topLevelWordPositions(query)
	fromAt, hasFrom := positions["from"]
	_, hasWhere := positions["where"]
	if !hasFrom || !hasWhere {
		return "", false
	}
	for _, word := range []string{"group", "order", "having", "union", "intersect", "except", "distinct", "window", "qualify"} {
		if _, ok := positions[word]; ok {
			return "", false
		}
	}
	if table, _ := identAt(query, skipSpaces(query, fromAt+len("from"))); !strings.EqualFold(table, "input") {
		return "", false
	}
	insertAt := len(query)
	for _, keyword := range []string{"limit", "offset"} {
		if pos, ok := positions[keyword]; ok {
			insertAt = min(insertAt, pos)
		}
	}
	trimmed := strings.TrimRight(query[:insertAt], " \t\r\n")
	semicolon := ""
	if insertAt == len(query) && strings.HasSuffix(trimmed, ";") {
		trimmed = strings.TrimRight(strings.TrimSuffix(trimmed, ";"), " \t\r\n")
		semicolon = ";"
	}
	tail := query[insertAt:]
	if tail != "" {
		tail = " " + tail
	}
	return trimmed + " order by " + rowOrderColumn + tail + semicolon, true
}
