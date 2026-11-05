package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

// Batched --stream.
//
// --stream answers the query for every NDJSON line on its own: a dataset
// per line, about 2.4ms each with DuckDB. Lines that are already waiting
// (a file, or a burst from tail -f) are run as one batch instead when that
// gives exactly the same output:
//
//   - the lines have the same fields with the same types, and each is a
//     single row (no arrays expanded into several rows), so the batch table
//     is what each line's own table would be;
//   - the query only selects and filters rows (DuckDB's plan has only
//     scans, filters and projections; the same allowlist as chunked
//     execution), so each line's rows do not depend on the other lines;
//   - the output format writes rows independently: CSV and JSON lines
//     always, tables once the header and column widths are set, Markdown
//     never (it repeats the header per line).
//
// A batch that fails is run again line by line, so errors are reported per
// line as before. A lone line arriving from tail -f is still answered at
// once.

const streamBatchMax = 2000

type streamLine struct {
	number int
	text   string
}

type streamRunner struct {
	session   *streamSession
	writer    *streamWriter
	opts      options
	query     string
	batchable map[string]bool // line signature -> query can run batched
}

func runStreamBatched(input io.Reader, session *streamSession, writer *streamWriter, opts options, query string) error {
	lines := make(chan streamLine, streamBatchMax*2)
	scanErr := make(chan error, 1)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(input)
		scanner.Buffer(make([]byte, 1024*64), 1024*1024*16)
		number := 0
		for scanner.Scan() {
			number++
			lines <- streamLine{number: number, text: strings.TrimSpace(scanner.Text())}
		}
		scanErr <- scanner.Err()
	}()

	runner := &streamRunner{session: session, writer: writer, opts: opts, query: query, batchable: map[string]bool{}}
	for {
		first, ok := <-lines
		if !ok {
			break
		}
		group := []streamLine{first}
		open := true
	drain:
		for len(group) < streamBatchMax {
			select {
			case line, more := <-lines:
				if !more {
					open = false
					break drain
				}
				group = append(group, line)
			default:
				break drain
			}
		}
		if err := runner.process(group); err != nil {
			return err
		}
		if !open {
			break
		}
	}
	return <-scanErr
}

// process answers a group of lines in input order, batching runs of lines
// that can share one query.
func (r *streamRunner) process(group []streamLine) error {
	for i := 0; i < len(group); {
		line := group[i]
		if line.text == "" {
			i++
			continue
		}
		signature, ok := streamLineSignature(line.text)
		if !ok || !r.canBatch() {
			if err := r.single(line, ""); err != nil {
				return err
			}
			i++
			continue
		}
		if _, known := r.batchable[signature]; !known {
			// The first line of a new shape runs alone and tells whether
			// the query can be batched for lines of that shape.
			if err := r.single(line, signature); err != nil {
				return err
			}
			i++
			continue
		}
		j := i + 1
		for j < len(group) {
			next := group[j]
			if next.text != "" {
				nextSignature, ok := streamLineSignature(next.text)
				if !ok || nextSignature != signature {
					break
				}
			}
			j++
		}
		if r.batchable[signature] && j-i > 1 {
			if err := r.batch(group[i:j]); err != nil {
				return err
			}
		} else {
			for _, each := range group[i:j] {
				if each.text != "" {
					if err := r.single(each, ""); err != nil {
						return err
					}
				}
			}
		}
		i = j
	}
	return nil
}

// canBatch reports whether the output format writes rows independently.
func (r *streamRunner) canBatch() bool {
	if r.opts.md {
		return false
	}
	if r.opts.json || r.opts.csv {
		return true
	}
	return r.writer.wroteHeader // table: widths come from the first rows
}

// single answers one line on its own, as --stream always did. With a
// signature, it also records whether lines of that shape can be batched.
func (r *streamRunner) single(line streamLine, signature string) error {
	ds, err := newDataset([]byte(line.text), "", fmt.Sprintf("stream:%d", line.number), r.opts.delimiter, r.session)
	if err != nil {
		fmt.Fprintf(os.Stderr, "stream line %d: %v\n", line.number, err)
		return nil
	}
	if signature != "" {
		r.batchable[signature] = r.queryShapeIsBatchable(ds)
	}
	result, err := ds.Query(context.Background(), r.query)
	ds.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "stream line %d: %v\n", line.number, err)
		return nil
	}
	return r.writer.Write(result)
}

// batch answers lines of one shape with one query, or line by line when
// the batch fails.
func (r *streamRunner) batch(lines []streamLine) error {
	var data strings.Builder
	for _, line := range lines {
		if line.text != "" {
			data.WriteString(line.text)
			data.WriteByte('\n')
		}
	}
	ds, err := newDataset([]byte(data.String()), "", fmt.Sprintf("stream:%d", lines[0].number), r.opts.delimiter, r.session)
	if err == nil {
		ds.streamBatch = true
		var result *QueryResult
		result, err = ds.Query(context.Background(), r.query)
		ds.Close()
		if err == nil {
			return r.writer.Write(result)
		}
	}
	for _, line := range lines {
		if line.text != "" {
			if err := r.single(line, ""); err != nil {
				return err
			}
		}
	}
	return nil
}

// streamLineSignature describes the table a JSON line becomes: its fields
// and their column types. ok is false for anything but a single JSON value
// that flattens into exactly one row.
func streamLineSignature(text string) (string, bool) {
	if !strings.HasPrefix(text, "{") {
		return "", false
	}
	var value any
	if json.Unmarshal([]byte(text), &value) != nil {
		return "", false
	}
	arrayPaths := map[string]struct{}{}
	rows := expandFlattenedRows("", value, arrayPaths)
	if len(rows) != 1 || len(arrayPaths) > 0 {
		return "", false
	}
	fields := make([]string, 0, len(rows[0]))
	for field := range rows[0] {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	var b strings.Builder
	for _, field := range fields {
		b.WriteString(field)
		b.WriteByte(0)
		b.WriteString(jsonColumnKind(field, rows))
		b.WriteByte(1)
	}
	return b.String(), true
}

// queryShapeIsBatchable decides from the query's plan whether lines of this
// shape can be batched. The plan must reflect the query, not the one line
// it was planned on: with statistics, DuckDB folds a filter no value of
// that line matches into an empty result, which says nothing about the
// query (status = 500 on a line with status 200). Statistics propagation is
// therefore off while planning.
func (r *streamRunner) queryShapeIsBatchable(ds *Dataset) bool {
	if _, err := ds.db.Exec("set disabled_optimizers = 'statistics_propagation'"); err != nil {
		return false
	}
	defer ds.db.Exec("reset disabled_optimizers")
	return ds.planIsChunkable(context.Background(), ds.rewrite(r.query))
}
