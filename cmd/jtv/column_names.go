package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Headerless CSV files and column names.
//
// Many exports have no header row. jtv detects them from the first rows:
// when the first row looks like the data below it, it is data, and the
// columns are named col1, col2, ... Users can rename columns with
// `rename col1 phone`; names are saved per file and applied whenever the
// file is opened again, through a temporary view over the dataset, so a
// rename is instant and never rebuilds a cache.

// csvHeaderMode is set by --csv-header: auto (default), yes or no.
var csvHeaderMode = "auto"

// headerSampleRows is how many records decide whether a CSV has a header.
const headerSampleRows = 200

func validHeaderMode(mode string) error {
	switch mode {
	case "auto", "yes", "no":
		return nil
	}
	return fmt.Errorf("--csv-header must be auto, yes or no, not %q", mode)
}

// csvHasHeader decides whether the first sampled record is a header.
func csvHasHeader(sample [][]string) bool {
	switch csvHeaderMode {
	case "yes":
		return true
	case "no":
		return false
	}
	return !looksHeaderless(sample)
}

// looksHeaderless reports whether the first record looks like data. It
// needs evidence: a column whose values are numbers or booleans, with the
// first record holding a value of that kind too. A word above numbers
// ("id" above 1, 2, 3) means a header, and so does a file of text columns
// only, where jtv cannot tell and keeps its usual assumption.
func looksHeaderless(sample [][]string) bool {
	if len(sample) < 2 {
		return false
	}
	first, rest := sample[0], sample[1:]
	evidence := false
	for j, value := range first {
		kind := scalarKind(value)
		if kind == "empty" {
			continue
		}
		column := columnKind(rest, j)
		switch column {
		case "number", "bool":
			if kind != column {
				return false // a name above typed data: a header
			}
			evidence = true
		}
	}
	return evidence
}

func scalarKind(value string) string {
	value = strings.TrimSpace(value)
	switch {
	case value == "":
		return "empty"
	case strings.EqualFold(value, "true") || strings.EqualFold(value, "false"):
		return "bool"
	case strings.ContainsAny(value, "0123456789"):
		if _, err := strconv.ParseFloat(value, 64); err == nil {
			return "number"
		}
	}
	return "text"
}

// columnKind is "number" or "bool" when every non-empty value of column j
// is of that kind, "text" when any is not, "empty" when there are none.
func columnKind(records [][]string, j int) string {
	kind := "empty"
	for _, record := range records {
		if j >= len(record) {
			continue
		}
		k := scalarKind(record[j])
		switch {
		case k == "empty":
		case kind == "empty":
			kind = k
		case k != kind:
			return "text"
		}
	}
	if kind != "number" && kind != "bool" && kind != "empty" {
		return "text"
	}
	return kind
}

// generatedHeaders names the columns of a headerless file col1, col2, ...
func generatedHeaders(sample [][]string) []string {
	width := 0
	for _, record := range sample {
		width = max(width, len(record))
	}
	headers := make([]string, width)
	for i := range headers {
		headers[i] = fmt.Sprintf("col%d", i+1)
	}
	return headers
}

// --- saved names ---

type savedColumnNames struct {
	Path  string            `json:"path"`
	Names map[string]string `json:"names"` // original name -> new name
}

func columnNamesFile(absPath string) (string, bool) {
	dir := os.Getenv("JTV_COLUMN_NAMES_DIR")
	if dir == "" {
		base, err := os.UserConfigDir()
		if err != nil {
			return "", false
		}
		dir = filepath.Join(base, "jtv", "columns")
	}
	return filepath.Join(dir, shortHash(absPath)+".json"), true
}

func loadColumnNames(absPath string) map[string]string {
	file, ok := columnNamesFile(absPath)
	if !ok {
		return nil
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return nil
	}
	var saved savedColumnNames
	if json.Unmarshal(data, &saved) != nil || saved.Path != absPath {
		return nil
	}
	return saved.Names
}

func saveColumnNames(absPath string, names map[string]string) error {
	file, ok := columnNamesFile(absPath)
	if !ok {
		return errors.New("no config directory to save column names in")
	}
	if len(names) == 0 {
		err := os.Remove(file)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(savedColumnNames{Path: absPath, Names: names}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(file, append(data, '\n'), 0o644)
}

// --- applying names ---

// baseFields is a dataset's field metadata under the original names.
type baseFields struct {
	fields        []string
	labels        map[string]string
	types         map[string]string
	samples       map[string]string
	arrays        map[string]bool
	textFields    map[string]bool
	keyTables     map[string]string
	keyCandidates map[string]keyColumn
	rawIsRowJSON  bool // raw is to_json of the row (CSV), so it is rebuilt
}

// fieldsOr returns the original field names, or fields before any rename.
func (b *baseFields) fieldsOr(fields []string) []string {
	if b == nil {
		return fields
	}
	return b.fields
}

// supportsRenames reports whether the dataset's input view can be renamed:
// datasets loaded into jtv_data with a row id, not stream sessions.
func (d *Dataset) supportsRenames() bool {
	return d.stream == nil && d.db != nil
}

// applyColumnNames shows the dataset's fields under new names, given as
// original name -> new name; an empty map restores the original names.
func (d *Dataset) applyColumnNames(names map[string]string) error {
	if !d.supportsRenames() {
		return errors.New("renaming columns is not supported here")
	}
	if d.base == nil {
		d.base = &baseFields{
			fields: d.Fields, labels: d.FieldLabels, types: d.FieldTypes, samples: d.FieldSamples,
			arrays: d.ArrayFields, textFields: d.textFields, keyTables: d.keyTables,
			keyCandidates: d.keyCandidates, rawIsRowJSON: d.csv,
		}
	}
	base := d.base
	if err := validateColumnNames(base.fields, names); err != nil {
		return err
	}
	logical := func(name string) string {
		if renamed, ok := names[name]; ok {
			return renamed
		}
		return name
	}

	if len(names) == 0 {
		if _, err := d.db.Exec("drop view if exists temp.input"); err != nil {
			return err
		}
	} else {
		var catalog string
		if err := d.db.QueryRow("select current_database()").Scan(&catalog); err != nil {
			return err
		}
		var selects, rawArgs []string
		for _, field := range d.physicalColumns() {
			selects = append(selects, quoteIdent(field)+" as "+quoteIdent(logical(field)))
			rawArgs = append(rawArgs, sqlString(logical(field)), quoteIdent(field))
		}
		raw := "raw"
		if base.rawIsRowJSON {
			raw = "json_object(" + strings.Join(rawArgs, ", ") + ") as raw"
		}
		selects = append(selects, raw, rowOrderColumn)
		view := "create or replace temp view input as select " + strings.Join(selects, ", ") +
			" from " + quoteIdent(catalog) + ".main.input"
		if _, err := d.db.Exec(view); err != nil {
			return err
		}
	}

	fieldSet := map[string]struct{}{}
	d.FieldLabels = map[string]string{}
	d.FieldTypes = map[string]string{}
	d.FieldSamples = map[string]string{}
	d.ArrayFields = map[string]bool{}
	d.textFields = map[string]bool{}
	d.keyTables = map[string]string{}
	d.keyCandidates = map[string]keyColumn{}
	for _, field := range base.fields {
		name := logical(field)
		fieldSet[name] = struct{}{}
		if label, ok := base.labels[field]; ok {
			if name != field {
				label = name
			}
			d.FieldLabels[name] = label
		}
		if v, ok := base.types[field]; ok {
			d.FieldTypes[name] = v
		}
		if v, ok := base.samples[field]; ok {
			d.FieldSamples[name] = v
		}
		if v, ok := base.arrays[field]; ok {
			d.ArrayFields[name] = v
		}
		lower, lowerName := strings.ToLower(field), strings.ToLower(name)
		if base.textFields[lower] {
			d.textFields[lowerName] = true
		}
		if table, ok := base.keyTables[lower]; ok {
			d.keyTables[lowerName] = table
		}
		if column, ok := base.keyCandidates[lower]; ok {
			d.keyCandidates[lowerName] = column
		}
	}
	d.Fields = sortedKeys(fieldSet)
	d.columnNames = names
	return nil
}

// physicalColumns are the dataset's own columns in table order, without
// raw and the row id.
func (d *Dataset) physicalColumns() []string {
	rows, err := d.db.Query("select column_name from information_schema.columns where table_name = 'jtv_data' order by ordinal_position")
	if err != nil {
		return nil
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var name string
		if rows.Scan(&name) == nil && name != "raw" {
			columns = append(columns, name)
		}
	}
	return columns
}

func validateColumnNames(fields []string, names map[string]string) error {
	known := map[string]bool{}
	for _, field := range fields {
		known[field] = true
	}
	taken := map[string]string{}
	for _, field := range fields {
		name := field
		if renamed, ok := names[field]; ok {
			name = renamed
		}
		lower := strings.ToLower(name)
		if other, dup := taken[lower]; dup {
			return fmt.Errorf("%s and %s would both be named %s", other, field, name)
		}
		taken[lower] = field
	}
	for original, name := range names {
		switch {
		case !known[original]:
			return fmt.Errorf("no field %s", original)
		case original == "raw":
			return errors.New("raw cannot be renamed")
		case strings.TrimSpace(name) == "" || name != strings.TrimSpace(name):
			return fmt.Errorf("invalid name %q", name)
		case strings.EqualFold(name, "raw") || strings.EqualFold(name, rowOrderColumn):
			return fmt.Errorf("%s is reserved", name)
		}
	}
	return nil
}

// renameCommand handles `rename`, `rename OLD NEW` and `rename reset`.
func (d *Dataset) renameCommand(arg string, out interface{ Write([]byte) (int, error) }) error {
	parts := strings.Fields(arg)
	current := map[string]string{}
	for original, name := range d.columnNames {
		current[original] = name
	}
	original := func(name string) string {
		for o, n := range current {
			if strings.EqualFold(n, name) {
				return o
			}
		}
		if d.base != nil {
			for _, field := range d.base.fields {
				if strings.EqualFold(field, name) {
					return field
				}
			}
		}
		for _, field := range d.Fields {
			if strings.EqualFold(field, name) {
				return field
			}
		}
		return name
	}

	switch {
	case len(parts) == 0:
		if len(current) == 0 {
			fmt.Fprintln(out, "no renamed fields; use: rename FIELD NEW_NAME")
			return nil
		}
		keys := make([]string, 0, len(current))
		for k := range current {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(out, "%s -> %s\n", k, current[k])
		}
		return nil
	case len(parts) == 1 && strings.EqualFold(parts[0], "reset"):
		current = map[string]string{}
	case len(parts) == 2:
		from := original(strings.Trim(parts[0], `"`))
		to := strings.Trim(parts[1], `"`)
		if to == from {
			delete(current, from)
		} else {
			current[from] = to
		}
	default:
		return errors.New("use: rename FIELD NEW_NAME, rename reset, or rename to list")
	}

	if err := d.applyColumnNames(current); err != nil {
		return err
	}
	if d.namesPath != "" {
		if err := saveColumnNames(d.namesPath, current); err != nil {
			return err
		}
	}
	if len(current) == 0 {
		fmt.Fprintln(out, "field names reset")
	} else if len(parts) == 2 {
		saved := ""
		if d.namesPath != "" {
			saved = " (saved for this file)"
		}
		fmt.Fprintf(out, "renamed %s to %s%s\n", original(parts[0]), strings.Trim(parts[1], `"`), saved)
	}
	return nil
}

// refreshSavedColumnNames re-applies the saved names when they changed,
// for a long-running jtv serve.
func (d *Dataset) refreshSavedColumnNames() {
	if d.namesPath == "" {
		return
	}
	names := loadColumnNames(d.namesPath)
	if fmt.Sprint(names) == fmt.Sprint(d.columnNames) || (len(names) == 0 && len(d.columnNames) == 0) {
		return
	}
	if names == nil {
		names = map[string]string{}
	}
	d.applyColumnNames(names)
}

// useSavedColumnNames applies the names saved for a local file, if any.
func (d *Dataset) useSavedColumnNames(path string) {
	if path == "" || !d.supportsRenames() {
		return
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return
	}
	d.namesPath = absPath
	if names := loadColumnNames(absPath); len(names) > 0 {
		// Saved names that no longer fit the file are ignored.
		d.applyColumnNames(names)
	}
}
