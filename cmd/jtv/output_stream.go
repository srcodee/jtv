package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Streaming output for -q.
//
// Results are written row by row as DuckDB produces them instead of being
// collected first, so a query returning 100 million rows needs no more
// memory than one returning ten. CSV, JSON and Markdown are byte-for-byte
// what the collecting writers produce. A table needs every row to size its
// columns; it is printed exactly as before up to tableStreamThreshold rows,
// and beyond that sized from the first rows, longer cells being truncated.

const tableStreamThreshold = 10000

type resultSink interface {
	Columns(columns []string) error
	Row(row []string, values []any) error
	Close() error
}

func streamQueryResult(ds *Dataset, opts options, stdout io.Writer) (err error) {
	out := stdout
	format := "table"
	switch {
	case opts.output != "":
		switch strings.ToLower(filepath.Ext(opts.output)) {
		case ".json":
			format = "json"
		case ".md", ".markdown":
			format = "md"
		case ".csv", "":
			format = "csv"
			if handled, err := tryCopyCSV(ds, opts.query, opts.output); handled {
				return err
			}
		default:
			return fmt.Errorf("unsupported output extension %q; use .csv, .json, or .md", filepath.Ext(opts.output))
		}
		file := &lazyFile{path: opts.output}
		defer func() {
			if closeErr := file.Close(); err == nil {
				err = closeErr
			}
		}()
		out = file
	case opts.json:
		format = "json"
	case opts.csv:
		format = "csv"
	case opts.md:
		format = "md"
	}

	buffered := bufio.NewWriterSize(out, 256<<10)
	var sink resultSink
	switch format {
	case "json":
		sink = &jsonSink{w: buffered}
	case "csv":
		sink = &csvSink{w: csv.NewWriter(buffered)}
	case "md":
		sink = &markdownSink{w: buffered}
	default:
		sink = &tableSink{w: buffered}
	}
	queryErr := ds.QueryEach(context.Background(), opts.query, sink.Columns, sink.Row)
	if queryErr != nil {
		// Like the collecting writers, print nothing for a failed query.
		return queryErr
	}
	if err := sink.Close(); err != nil {
		return err
	}
	if err := buffered.Flush(); err != nil {
		return err
	}
	if file, ok := out.(*lazyFile); ok {
		return file.ensureOpen() // an empty result still writes a file
	}
	return nil
}

// lazyFile creates its file on the first write, so a query that fails
// before producing output leaves no file behind, as before streaming.
type lazyFile struct {
	path string
	file *os.File
}

func (f *lazyFile) ensureOpen() error {
	if f.file != nil {
		return nil
	}
	file, err := os.Create(f.path)
	if err != nil {
		return err
	}
	f.file = file
	return nil
}

func (f *lazyFile) Write(p []byte) (int, error) {
	if err := f.ensureOpen(); err != nil {
		return 0, err
	}
	return f.file.Write(p)
}

func (f *lazyFile) Close() error {
	if f.file == nil {
		return nil
	}
	return f.file.Close()
}

type csvSink struct {
	w *csv.Writer
}

func (s *csvSink) Columns(columns []string) error  { return s.w.Write(columns) }
func (s *csvSink) Row(row []string, _ []any) error { return s.w.Write(row) }
func (s *csvSink) Close() error {
	s.w.Flush()
	return s.w.Error()
}

type markdownSink struct {
	w       io.Writer
	columns int
}

func (s *markdownSink) Columns(columns []string) error {
	s.columns = len(columns)
	if s.columns == 0 {
		return nil
	}
	writeMarkdownRow(s.w, columns)
	separator := make([]string, len(columns))
	for i := range separator {
		separator[i] = "---"
	}
	writeMarkdownRow(s.w, separator)
	return nil
}

func (s *markdownSink) Row(row []string, _ []any) error {
	if s.columns > 0 {
		writeMarkdownRow(s.w, row)
	}
	return nil
}

func (s *markdownSink) Close() error { return nil }

// jsonSink writes the same indented array as writeJSON, one object at a time.
type jsonSink struct {
	w       io.Writer
	columns []string
	rows    int
}

func (s *jsonSink) Columns(columns []string) error {
	s.columns = columns
	return nil
}

func (s *jsonSink) Row(_ []string, values []any) error {
	object := make(map[string]any, len(s.columns))
	for i, column := range s.columns {
		if i < len(values) {
			object[column] = jsonValue(values[i])
		}
	}
	encoded, err := json.MarshalIndent(object, "  ", "  ")
	if err != nil {
		return err
	}
	prefix := ",\n  "
	if s.rows == 0 {
		prefix = "[\n  "
	}
	s.rows++
	if _, err := io.WriteString(s.w, prefix); err != nil {
		return err
	}
	_, err = s.w.Write(encoded)
	return err
}

func (s *jsonSink) Close() error {
	end := "\n]\n"
	if s.rows == 0 {
		end = "[]\n"
	}
	_, err := io.WriteString(s.w, end)
	return err
}

// tableSink buffers up to tableStreamThreshold rows, then streams.
type tableSink struct {
	w         io.Writer
	columns   []string
	rows      [][]string
	widths    []int
	streaming bool
	count     int
}

func (s *tableSink) Columns(columns []string) error {
	s.columns = columns
	return nil
}

func (s *tableSink) Row(row []string, _ []any) error {
	s.count++
	if s.streaming {
		printDataRow(s.w, row, s.widths)
		return nil
	}
	s.rows = append(s.rows, row)
	if len(s.rows) > tableStreamThreshold && len(s.columns) > 0 {
		s.streaming = true
		s.widths = tableWidths(s.columns, s.rows)
		printBorder(s.w, s.widths)
		printRow(s.w, s.columns, s.widths)
		printBorder(s.w, s.widths)
		for _, buffered := range s.rows {
			printDataRow(s.w, buffered, s.widths)
		}
		s.rows = nil
	}
	return nil
}

func (s *tableSink) Close() error {
	if !s.streaming {
		printTable(s.w, &QueryResult{Columns: s.columns, Rows: s.rows})
		return nil
	}
	printBorder(s.w, s.widths)
	fmt.Fprintf(s.w, "%d row(s)\n", s.count)
	return nil
}
