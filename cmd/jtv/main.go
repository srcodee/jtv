package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	prompt "github.com/c-bata/go-prompt"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

const version = "0.1.0"

type options struct {
	file       string
	query      string
	output     string
	configPath string
	csv        bool
	json       bool
	stream     bool
	noConfig   bool
	configSet  bool
	version    bool
	pageSize   int
}

func defaultOptions(args []string) (options, error) {
	opts := options{pageSize: defaultPageSize}
	configPath, noConfig, configSet := configArgs(args)
	opts.configPath = configPath
	opts.noConfig = noConfig
	opts.configSet = configSet
	if noConfig {
		return opts, nil
	}
	if opts.configPath == "" {
		path, err := defaultConfigPath()
		if err != nil {
			return opts, nil
		}
		opts.configPath = path
	}
	return applyConfigFile(opts)
}

func configArgs(args []string) (string, bool, bool) {
	var path string
	noConfig := false
	configSet := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--no-config" || arg == "-no-config":
			noConfig = true
		case arg == "--config" || arg == "-config":
			if i+1 < len(args) {
				path = args[i+1]
				configSet = true
				i++
			}
		case strings.HasPrefix(arg, "--config="):
			path = strings.TrimPrefix(arg, "--config=")
			configSet = true
		case strings.HasPrefix(arg, "-config="):
			path = strings.TrimPrefix(arg, "-config=")
			configSet = true
		}
	}
	return path, noConfig, configSet
}

func defaultConfigPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "jtv", "config.toml"), nil
}

func applyConfigFile(opts options) (options, error) {
	data, err := os.ReadFile(opts.configPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if opts.configSet {
				return opts, err
			}
			return opts, nil
		}
		return opts, err
	}
	cfg, err := parseConfig(data)
	if err != nil {
		return opts, fmt.Errorf("%s: %w", opts.configPath, err)
	}
	if cfg.pageSize > 0 {
		opts.pageSize = cfg.pageSize
	}
	switch cfg.output {
	case "csv":
		opts.csv = true
		opts.json = false
	case "json":
		opts.csv = false
		opts.json = true
	case "table", "":
		opts.csv = false
		opts.json = false
	default:
		return opts, fmt.Errorf("%s: output must be table, csv, or json", opts.configPath)
	}
	return opts, nil
}

type configFile struct {
	output   string
	pageSize int
}

func parseConfig(data []byte) (configFile, error) {
	var cfg configFile
	scanner := bufio.NewScanner(bytes.NewReader(data))
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := stripConfigComment(strings.TrimSpace(scanner.Text()))
		if line == "" || strings.HasPrefix(line, "[") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return cfg, fmt.Errorf("line %d: expected key = value", lineNumber)
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		switch key {
		case "output":
			cfg.output = strings.ToLower(value)
		case "pagesize", "page_size":
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 {
				return cfg, fmt.Errorf("line %d: pagesize must be >= 1", lineNumber)
			}
			cfg.pageSize = n
		default:
			return cfg, fmt.Errorf("line %d: unknown config key %q", lineNumber, key)
		}
	}
	return cfg, scanner.Err()
}

func stripConfigComment(line string) string {
	inQuote := byte(0)
	for i := 0; i < len(line); i++ {
		ch := line[i]
		if inQuote != 0 {
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		if ch == '"' || ch == '\'' {
			inQuote = ch
			continue
		}
		if ch == '#' {
			return strings.TrimSpace(line[:i])
		}
	}
	return line
}

func resolveOutputFlagOverrides(fs *flag.FlagSet, opts *options) {
	visited := map[string]bool{}
	fs.Visit(func(f *flag.Flag) {
		visited[f.Name] = true
	})
	if visited["csv"] && !visited["json"] {
		opts.json = false
	}
	if visited["json"] && !visited["csv"] {
		opts.csv = false
	}
}

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	opts, err := defaultOptions(args)
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("jtv", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&opts.file, "f", "", "input JSON/NDJSON/CSV file, or - for stdin")
	fs.StringVar(&opts.query, "q", "", "SQL query to execute")
	fs.StringVar(&opts.output, "o", "", "write output to file; format is inferred from extension")
	fs.StringVar(&opts.configPath, "config", opts.configPath, "config file path")
	fs.BoolVar(&opts.csv, "csv", opts.csv, "write query result as CSV")
	fs.BoolVar(&opts.json, "json", opts.json, "write query result as JSON")
	fs.BoolVar(&opts.stream, "stream", false, "read NDJSON continuously and run query for each line")
	fs.BoolVar(&opts.noConfig, "no-config", opts.noConfig, "ignore config file")
	fs.BoolVar(&opts.version, "version", false, "print version and exit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	resolveOutputFlagOverrides(fs, &opts)

	if opts.version {
		printVersion(stdout)
		return nil
	}

	if opts.stream {
		return runStream(fs.Args(), stdin, stdout, opts)
	}

	data, source, err := readInput(fs.Args(), stdin, opts.file)
	if err != nil {
		return err
	}

	ds, err := NewDataset(data, source)
	if err != nil {
		return err
	}
	defer ds.Close()

	if opts.query == "" {
		return runInteractive(ds, stdin, stdout, opts.pageSize)
	}

	if handled, err := runQueryCommand(ds, stdout, opts.query); handled {
		return err
	}

	result, err := ds.Query(context.Background(), opts.query)
	if err != nil {
		return err
	}
	if opts.output != "" {
		return writeResultFile(opts.output, result)
	}
	if opts.json {
		return writeJSON(stdout, result)
	}
	if opts.csv {
		return writeCSV(stdout, result)
	}
	printTable(stdout, result)
	return nil
}

func runQueryCommand(ds *Dataset, out io.Writer, query string) (bool, error) {
	command, arg := splitCommand(strings.TrimSpace(query))
	switch command {
	case "ls", "schema", ".ls", ".schema":
		printSchema(out, ds.Fields, ds.FieldLabels, arg)
		return true, nil
	case "preview", ".preview":
		limit := "10"
		if arg != "" {
			limit = arg
		}
		result, err := ds.Query(context.Background(), "select * limit "+limit)
		if err != nil {
			return true, err
		}
		printTable(out, result)
		return true, nil
	default:
		return false, nil
	}
}

func printVersion(out io.Writer) {
	fmt.Fprintf(out, "jtv %s\n", version)
}

func readInput(args []string, stdin io.Reader, file string) ([]byte, string, error) {
	if len(args) > 0 {
		return nil, "", errors.New("input file must use -f")
	}
	if file == "-" {
		data, err := io.ReadAll(stdin)
		return data, "stdin", err
	}
	if file == "" {
		if isTerminal(stdin) {
			return nil, "", errors.New("missing required -f input file")
		}
		data, err := io.ReadAll(stdin)
		return data, "stdin", err
	}
	data, err := os.ReadFile(file)
	return data, file, err
}

func openStreamInput(args []string, stdin io.Reader, file string) (io.ReadCloser, error) {
	if len(args) > 0 {
		return nil, errors.New("input file must use -f")
	}
	if file == "" || file == "-" {
		return io.NopCloser(stdin), nil
	}
	return os.Open(file)
}

func runStream(args []string, stdin io.Reader, stdout io.Writer, opts options) error {
	if opts.query == "" {
		return errors.New("--stream requires -q")
	}
	if opts.output != "" {
		return errors.New("--stream does not support -o yet; redirect stdout instead")
	}

	input, err := openStreamInput(args, stdin, opts.file)
	if err != nil {
		return err
	}
	defer input.Close()

	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 1024*64), 1024*1024*16)
	writer := newStreamWriter(stdout, opts)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		ds, err := NewDataset([]byte(line), fmt.Sprintf("stream:%d", lineNumber))
		if err != nil {
			fmt.Fprintf(os.Stderr, "stream line %d: %v\n", lineNumber, err)
			continue
		}
		result, err := ds.Query(context.Background(), opts.query)
		ds.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "stream line %d: %v\n", lineNumber, err)
			continue
		}
		if err := writer.Write(result); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return writer.Close()
}

func isTerminal(r io.Reader) bool {
	file, ok := r.(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

func runInteractive(ds *Dataset, stdin io.Reader, out io.Writer, pageSize int) error {
	restore := restoreTerminalOnExit(stdin)
	defer restore()

	printWelcome(out, ds)

	var last *QueryResult
	pager := pageState{size: pageSize}
	var history []string
	completer := makeCompleter(ds.Fields)
	reader := bufio.NewReader(stdin)
	useAutocomplete := true
	for {
		line, err := readInteractiveLine(reader, out, completer, history, &useAutocomplete)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		command, arg := splitCommand(line)
		switch {
		case command == ".quit" || command == ".exit" || command == "quit" || command == "exit":
			return nil
		case command == "next" || command == "n":
			if !pager.active {
				fmt.Fprintln(out, "error: no paged query")
				continue
			}
			result, err := runPage(ds, &pager, pager.page+1)
			if err != nil {
				fmt.Fprintln(out, "error:", err)
				continue
			}
			last = result
			printPagedTable(out, result, pager)
			continue
		case command == "prev" || command == "p":
			if !pager.active {
				fmt.Fprintln(out, "error: no paged query")
				continue
			}
			result, err := runPage(ds, &pager, pager.page-1)
			if err != nil {
				fmt.Fprintln(out, "error:", err)
				continue
			}
			last = result
			printPagedTable(out, result, pager)
			continue
		case command == "page":
			if !pager.active {
				fmt.Fprintln(out, "error: no paged query")
				continue
			}
			page, err := strconv.Atoi(arg)
			if err != nil || page < 1 {
				fmt.Fprintln(out, "error: page number must be >= 1")
				continue
			}
			result, err := runPage(ds, &pager, page)
			if err != nil {
				fmt.Fprintln(out, "error:", err)
				continue
			}
			last = result
			printPagedTable(out, result, pager)
			continue
		case command == "pagesize":
			size, err := strconv.Atoi(arg)
			if err != nil || size < 1 {
				fmt.Fprintln(out, "error: pagesize must be >= 1")
				continue
			}
			pager.size = size
			if pager.active {
				result, err := runPage(ds, &pager, 1)
				if err != nil {
					fmt.Fprintln(out, "error:", err)
					continue
				}
				last = result
				printPagedTable(out, result, pager)
			} else {
				fmt.Fprintf(out, "pagesize set to %d\n", pager.size)
			}
			continue
		case command == "bar" || command == "top" || command == "hist" || command == "line" || command == "chart":
			if err := runChartCommand(ds, out, command, arg); err != nil {
				fmt.Fprintln(out, "error:", err)
			}
			continue
		case command == ".clear" || command == "clear":
			clearScreen(out)
			continue
		case command == ".help" || command == "help":
			printHelp(out)
			continue
		case command == ".schema" || command == ".ls" || command == "schema" || command == "ls":
			printSchema(out, ds.Fields, ds.FieldLabels, arg)
			continue
		case command == ".preview" || command == "preview":
			query := "select * limit 10"
			if arg != "" {
				query = "select * limit " + arg
			}
			result, err := ds.Query(context.Background(), query)
			if err != nil {
				fmt.Fprintln(out, "error:", err)
				continue
			}
			printTable(out, result)
			continue
		case command == ".csv" || command == "csv" || command == ".json" || command == "json" || command == "export":
			if last == nil {
				fmt.Fprintln(out, "error: no previous query result")
				continue
			}
			path := arg
			if path == "" {
				fmt.Fprintln(out, "error: missing export file path")
				continue
			}
			if err := writeInteractiveExport(command, path, last); err != nil {
				fmt.Fprintln(out, "error:", err)
				continue
			}
			fmt.Fprintf(out, "wrote %s\n", path)
			continue
		case strings.HasPrefix(line, "."):
			fmt.Fprintln(out, "error: unknown command")
			continue
		}

		query, paged := prepareInteractiveQuery(line, pager.size)
		result, err := ds.Query(context.Background(), query)
		if err != nil {
			fmt.Fprintln(out, "error:", err)
			continue
		}
		history = appendHistory(history, line)
		last = result
		if paged {
			pager.active = true
			pager.query = line
			pager.page = 1
			pager.fileRows = ds.RowCount
			if pager.size == 0 {
				pager.size = defaultPageSize
			}
			totalRows, err := ds.Count(context.Background(), line)
			if err != nil {
				fmt.Fprintln(out, "error:", err)
				pager.active = false
				continue
			}
			pager.totalRows = totalRows
			printPagedTable(out, result, pager)
		} else {
			pager.active = false
			printTable(out, result)
		}
	}
}

const defaultPageSize = 10

type pageState struct {
	active    bool
	query     string
	page      int
	size      int
	totalRows int
	fileRows  int
}

func prepareInteractiveQuery(query string, pageSize int) (string, bool) {
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	trimmed := strings.TrimSpace(query)
	if !isSelectQuery(trimmed) || hasTopLevelLimit(trimmed) {
		return query, false
	}
	return appendLimitOffset(trimmed, pageSize, 0), true
}

func runPage(ds *Dataset, pager *pageState, page int) (*QueryResult, error) {
	if pager.size <= 0 {
		pager.size = defaultPageSize
	}
	totalPages := pager.totalPages()
	if page < 1 {
		return nil, fmt.Errorf("already at first page (1/%d)", totalPages)
	}
	if totalPages > 0 && page > totalPages {
		return nil, fmt.Errorf("already at last page (%d/%d)", pager.page, totalPages)
	}
	offset := (page - 1) * pager.size
	query := appendLimitOffset(pager.query, pager.size, offset)
	result, err := ds.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	if len(result.Rows) == 0 && page > 1 {
		return nil, fmt.Errorf("already at last page (%d/%d)", pager.page, max(totalPages, 1))
	}
	pager.page = page
	return result, nil
}

func (p pageState) totalPages() int {
	if p.totalRows == 0 {
		return 1
	}
	if p.size <= 0 {
		return 1
	}
	return (p.totalRows + p.size - 1) / p.size
}

func (p pageState) showingRange(rowCount int) (int, int) {
	if p.totalRows == 0 || rowCount == 0 {
		return 0, 0
	}
	start := (p.page-1)*p.size + 1
	end := start + rowCount - 1
	if end > p.totalRows {
		end = p.totalRows
	}
	return start, end
}

func appendLimitOffset(query string, limit, offset int) string {
	trimmed := strings.TrimSpace(query)
	semicolon := ""
	if strings.HasSuffix(trimmed, ";") {
		trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
		semicolon = ";"
	}
	return fmt.Sprintf("%s limit %d offset %d%s", trimmed, limit, offset, semicolon)
}

func isSelectQuery(query string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(query)), "select ")
}

func printPagedTable(out io.Writer, result *QueryResult, pager pageState) {
	printTable(out, result)
	if pager.totalRows <= pager.size {
		return
	}
	start, end := pager.showingRange(len(result.Rows))
	totalPages := pager.totalPages()
	fmt.Fprintf(
		out,
		"page %d/%d | showing %d-%d of %d result row(s) | file rows: %d | pagesize: %d\n",
		pager.page,
		totalPages,
		start,
		end,
		pager.totalRows,
		pager.fileRows,
		pager.size,
	)

	commands := make([]string, 0, 4)
	if pager.page < totalPages {
		commands = append(commands, "next/n")
	}
	if pager.page > 1 {
		commands = append(commands, "prev/p")
	}
	commands = append(commands, "page N", "pagesize N")
	fmt.Fprintf(out, "pagination: %s\n", strings.Join(commands, " | "))
	if pager.page == totalPages && totalPages > 1 {
		fmt.Fprintln(out, "end of results")
	}
}

func applyInteractiveDefaults(query string) string {
	trimmed := strings.TrimSpace(query)
	if !strings.EqualFold(trimmed, "select *") && !strings.EqualFold(trimmed, "select *;") {
		return query
	}
	if strings.HasSuffix(trimmed, ";") {
		return strings.TrimSuffix(trimmed, ";") + " limit 10;"
	}
	return trimmed + " limit 10"
}

func printWelcome(out io.Writer, ds *Dataset) {
	fmt.Fprintf(out, "jtv %s\n", ds.Source)
	fmt.Fprintf(out, "%d rows, %d fields\n", ds.RowCount, visibleFieldCount(ds.Fields))
	fmt.Fprintln(out, "type help for commands, exit to quit")
	fmt.Fprintln(out, "")
}

func splitCommand(line string) (string, string) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return "", ""
	}
	command := strings.ToLower(parts[0])
	arg := strings.TrimSpace(strings.TrimPrefix(line, parts[0]))
	return command, arg
}

func readInteractiveLine(reader *bufio.Reader, out io.Writer, completer prompt.Completer, history []string, useAutocomplete *bool) (string, error) {
	if *useAutocomplete {
		line, ok := safePromptInput("jtv> ", completer, history)
		if ok {
			return line, nil
		}
		*useAutocomplete = false
		fmt.Fprintln(out, "autocomplete unavailable in this terminal; using plain SQL prompt")
	}

	fmt.Fprint(out, "jtv> ")
	return reader.ReadString('\n')
}

func safePromptInput(prefix string, completer prompt.Completer, history []string) (line string, ok bool) {
	defer func() {
		if recover() != nil {
			line = ""
			ok = false
		}
	}()
	return prompt.Input(prefix, completer, prompt.OptionHistory(history)), true
}

func appendHistory(history []string, line string) []string {
	line = strings.TrimSpace(line)
	if line == "" {
		return history
	}
	if len(history) > 0 && history[len(history)-1] == line {
		return history
	}
	return append(history, line)
}

func clearScreen(out io.Writer) {
	fmt.Fprint(out, "\033[H\033[2J\033[3J")
}

func restoreTerminalOnExit(stdin io.Reader) func() {
	restoreStdin := captureTerminalState(stdin)
	restoreTTY := captureDevTTYState()
	return func() {
		restoreTTY()
		restoreStdin()
		forceTerminalSane(stdin)
		forceDevTTYSane()
	}
}

func captureTerminalState(stdin io.Reader) func() {
	file, ok := stdin.(*os.File)
	if !ok {
		return func() {}
	}
	return captureTerminalFD(int(file.Fd()))
}

func captureDevTTYState() func() {
	file, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		return func() {}
	}
	restore := captureTerminalFD(int(file.Fd()))
	return func() {
		restore()
		_ = file.Close()
	}
}

func captureTerminalFD(fd int) func() {
	state, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return func() {}
	}
	return func() {
		_ = unix.SetNonblock(fd, false)
		_ = unix.IoctlSetTermios(fd, unix.TCSETS, state)
	}
}

func forceTerminalSane(stdin io.Reader) {
	file, ok := stdin.(*os.File)
	if !ok {
		return
	}
	forceTerminalFDSane(int(file.Fd()))
}

func forceDevTTYSane() {
	file, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		return
	}
	defer file.Close()
	forceTerminalFDSane(int(file.Fd()))
}

func forceTerminalFDSane(fd int) {
	state, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return
	}
	_ = unix.SetNonblock(fd, false)
	state.Iflag |= unix.ICRNL | unix.IXON | unix.BRKINT
	state.Oflag |= unix.OPOST | unix.ONLCR
	state.Lflag |= unix.ECHO | unix.ICANON | unix.ISIG | unix.IEXTEN
	state.Lflag &^= unix.ECHONL
	state.Cc[unix.VMIN] = 1
	state.Cc[unix.VTIME] = 0
	_ = unix.IoctlSetTermios(fd, unix.TCSETS, state)
}

func printHelp(out io.Writer) {
	fmt.Fprintln(out, "commands:")
	fmt.Fprintln(out, "  ls, schema          list detected fields")
	fmt.Fprintln(out, "  ls TEXT             search detected fields")
	fmt.Fprintln(out, "  ls TEXT -n 20       search and limit fields")
	fmt.Fprintln(out, "  preview [N]         show the first N rows")
	fmt.Fprintln(out, "  next, n             show next page")
	fmt.Fprintln(out, "  prev, p             show previous page")
	fmt.Fprintln(out, "  page N              jump to page N")
	fmt.Fprintln(out, "  pagesize N          set page size")
	fmt.Fprintln(out, "  bar FIELD           count by FIELD as bar chart")
	fmt.Fprintln(out, "  top FIELD [N]       top values by count")
	fmt.Fprintln(out, "  hist FIELD          histogram for numeric FIELD")
	fmt.Fprintln(out, "  line X_FIELD Y_FIELD sparkline trend")
	fmt.Fprintln(out, "  line full X Y        multiline connected line")
	fmt.Fprintln(out, "  line points X Y      point/scatter view")
	fmt.Fprintln(out, "  bar SELECT ...      bar chart from SQL: label, value")
	fmt.Fprintln(out, "  chart bar SELECT    same as bar SELECT")
	fmt.Fprintln(out, "  csv FILE            export the last query result to CSV")
	fmt.Fprintln(out, "  json FILE           export the last query result to JSON")
	fmt.Fprintln(out, "  export FILE         export by file extension: .csv or .json")
	fmt.Fprintln(out, "  clear               clear the screen")
	fmt.Fprintln(out, "  exit, quit          exit")
	fmt.Fprintln(out, "")
	fmt.Fprintln(out, "examples:")
	fmt.Fprintln(out, "  select * limit 10")
	fmt.Fprintln(out, "  select user.name, user.email limit 10")
	fmt.Fprintln(out, "  select status, count(*) group by status")
	fmt.Fprintln(out, "  bar comments.likes")
	fmt.Fprintln(out, "  top comments.user.username 10")
	fmt.Fprintln(out, "  hist comments.likes")
	fmt.Fprintln(out, "  line comments.id comments.likes")
	fmt.Fprintln(out, "  line full comments.id comments.likes")
	fmt.Fprintln(out, "  line points comments.id comments.likes")
	fmt.Fprintln(out, "  line select comments.id, comments.likes order by comments.id")
	fmt.Fprintln(out, "  line full select comments.id, comments.likes order by comments.id")
	fmt.Fprintln(out, "  bar select comments.likes, count(*) as total group by comments.likes")
	fmt.Fprintln(out, "  chart bar select comments.user.username, count(*) as total group by comments.user.username order by total desc limit 10")
}

func printSchema(out io.Writer, fields []string, labels map[string]string, args string) {
	filter, limit := parseLSArgs(args)
	matches := make([]string, 0, len(fields))
	for _, field := range fields {
		if field == "raw" {
			continue
		}
		label := labels[field]
		if label == "" {
			label = field
		}
		if filter != "" &&
			!strings.Contains(strings.ToLower(field), filter) &&
			!strings.Contains(strings.ToLower(label), filter) {
			continue
		}
		matches = append(matches, label)
	}

	if filter == "" {
		fmt.Fprintln(out, "fields:")
	} else {
		fmt.Fprintf(out, "fields matching %q:\n", filter)
	}
	shown := len(matches)
	if limit > 0 && shown > limit {
		shown = limit
	}
	for _, field := range matches[:shown] {
		fmt.Fprintln(out, field)
	}
	if len(matches) == 0 {
		fmt.Fprintln(out, "(none)")
		return
	}
	if shown < len(matches) {
		fmt.Fprintf(out, "%d of %d field(s), use -n %d to show more\n", shown, len(matches), len(matches))
		return
	}
	fmt.Fprintf(out, "%d field(s)\n", len(matches))
}

func parseLSArgs(args string) (string, int) {
	parts := strings.Fields(args)
	filterParts := make([]string, 0, len(parts))
	limit := 0
	for i := 0; i < len(parts); i++ {
		if parts[i] == "-n" && i+1 < len(parts) {
			if n, err := strconv.Atoi(parts[i+1]); err == nil && n > 0 {
				limit = n
			}
			i++
			continue
		}
		filterParts = append(filterParts, parts[i])
	}
	return strings.ToLower(strings.Join(filterParts, " ")), limit
}

func visibleFieldCount(fields []string) int {
	count := 0
	for _, field := range fields {
		if field != "raw" {
			count++
		}
	}
	return count
}

func makeCompleter(fields []string) prompt.Completer {
	keywords := []prompt.Suggest{
		{Text: "select", Description: "SQL SELECT"},
		{Text: "from", Description: "SQL FROM"},
		{Text: "input", Description: "JSON input table"},
		{Text: "where", Description: "filter rows"},
		{Text: "group by", Description: "group rows"},
		{Text: "order by", Description: "sort rows"},
		{Text: "limit", Description: "limit rows"},
		{Text: "count", Description: "aggregate count"},
		{Text: "sum", Description: "aggregate sum"},
		{Text: "avg", Description: "aggregate average"},
		{Text: "min", Description: "aggregate minimum"},
		{Text: "max", Description: "aggregate maximum"},
		{Text: "ls", Description: "list fields"},
		{Text: "schema", Description: "list fields"},
		{Text: "preview", Description: "show rows"},
		{Text: "next", Description: "next page"},
		{Text: "n", Description: "next page"},
		{Text: "prev", Description: "previous page"},
		{Text: "p", Description: "previous page"},
		{Text: "page", Description: "jump to page"},
		{Text: "pagesize", Description: "set page size"},
		{Text: "bar", Description: "bar chart"},
		{Text: "top", Description: "top values"},
		{Text: "hist", Description: "histogram"},
		{Text: "line", Description: "line chart"},
		{Text: "chart", Description: "chart command"},
		{Text: "csv", Description: "export last result"},
		{Text: "json", Description: "export last result"},
		{Text: "export", Description: "export last result"},
		{Text: "help", Description: "show help"},
		{Text: ".schema", Description: "show detected fields"},
		{Text: ".ls", Description: "list fields"},
		{Text: ".preview", Description: "show sample rows"},
		{Text: ".csv", Description: "export last result"},
		{Text: ".json", Description: "export last result"},
		{Text: ".clear", Description: "clear screen"},
		{Text: "clear", Description: "clear screen"},
		{Text: ".quit", Description: "exit"},
		{Text: "exit", Description: "exit"},
		{Text: "quit", Description: "exit"},
	}
	for _, field := range fields {
		keywords = append(keywords, prompt.Suggest{Text: field, Description: "JSON field"})
	}
	return func(d prompt.Document) []prompt.Suggest {
		word := d.GetWordBeforeCursor()
		return prompt.FilterHasPrefix(keywords, word, true)
	}
}

func writeInteractiveExport(command, path string, result *QueryResult) error {
	switch command {
	case "csv", ".csv":
		return writeCSVFile(path, result)
	case "json", ".json":
		return writeJSONFile(path, result)
	default:
		return writeResultFile(path, result)
	}
}

func writeResultFile(path string, result *QueryResult) error {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		return writeJSONFile(path, result)
	case ".csv", "":
		return writeCSVFile(path, result)
	default:
		return fmt.Errorf("unsupported output extension %q; use .csv or .json", filepath.Ext(path))
	}
}

func writeCSVFile(path string, result *QueryResult) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return writeCSV(file, result)
}

func writeJSONFile(path string, result *QueryResult) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return writeJSON(file, result)
}

func writeCSV(w io.Writer, result *QueryResult) error {
	writer := csv.NewWriter(w)
	if err := writer.Write(result.Columns); err != nil {
		return err
	}
	for _, row := range result.Rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeJSON(w io.Writer, result *QueryResult) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(resultRowsAsObjects(result))
}

func jsonValue(value any) any {
	switch v := value.(type) {
	case []byte:
		return string(v)
	default:
		return v
	}
}

type streamWriter struct {
	out         io.Writer
	opts        options
	csvWriter   *csv.Writer
	header      []string
	widths      []int
	wroteHeader bool
}

func newStreamWriter(out io.Writer, opts options) *streamWriter {
	w := &streamWriter{out: out, opts: opts}
	if opts.csv {
		w.csvWriter = csv.NewWriter(out)
	}
	return w
}

func (w *streamWriter) Write(result *QueryResult) error {
	if w.opts.json {
		return w.writeJSONLines(result)
	}
	if w.opts.csv {
		return w.writeCSVRows(result)
	}
	return w.writeTableRows(result)
}

func (w *streamWriter) Close() error {
	if w.csvWriter != nil {
		w.csvWriter.Flush()
		return w.csvWriter.Error()
	}
	return nil
}

func (w *streamWriter) writeCSVRows(result *QueryResult) error {
	if !w.wroteHeader {
		if err := w.csvWriter.Write(result.Columns); err != nil {
			return err
		}
		w.header = append([]string(nil), result.Columns...)
		w.wroteHeader = true
	}
	for _, row := range result.Rows {
		if err := w.csvWriter.Write(row); err != nil {
			return err
		}
	}
	w.csvWriter.Flush()
	return w.csvWriter.Error()
}

func (w *streamWriter) writeJSONLines(result *QueryResult) error {
	for _, row := range resultRowsAsObjects(result) {
		raw, err := json.Marshal(row)
		if err != nil {
			return err
		}
		fmt.Fprintln(w.out, string(raw))
	}
	return nil
}

func (w *streamWriter) writeTableRows(result *QueryResult) error {
	if len(result.Rows) == 0 {
		return nil
	}
	if !w.wroteHeader {
		w.header = append([]string(nil), result.Columns...)
		w.widths = tableWidths(result.Columns, result.Rows)
		printBorder(w.out, w.widths)
		printRow(w.out, result.Columns, w.widths)
		printBorder(w.out, w.widths)
		w.wroteHeader = true
	}
	for _, row := range result.Rows {
		printDataRow(w.out, row, w.widths)
	}
	return nil
}

func resultRowsAsObjects(result *QueryResult) []map[string]any {
	rows := make([]map[string]any, 0, len(result.Values))
	for _, values := range result.Values {
		row := make(map[string]any, len(result.Columns))
		for i, column := range result.Columns {
			if i < len(values) {
				row[column] = jsonValue(values[i])
			}
		}
		rows = append(rows, row)
	}
	return rows
}

func printTable(w io.Writer, result *QueryResult) {
	if len(result.Columns) == 0 {
		fmt.Fprintln(w, "(no columns)")
		return
	}

	widths := tableWidths(result.Columns, result.Rows)
	if len(result.Rows) == 0 {
		ensureNoRowsMessageFits(widths)
	}

	printBorder(w, widths)
	printRow(w, result.Columns, widths)
	printBorder(w, widths)
	if len(result.Rows) == 0 {
		printSpanningRow(w, "no rows found", widths)
		printBorder(w, widths)
		fmt.Fprintln(w, "0 row(s)")
		return
	}
	for _, row := range result.Rows {
		printDataRow(w, row, widths)
	}
	printBorder(w, widths)
	fmt.Fprintf(w, "%d row(s)\n", len(result.Rows))
}

func tableWidths(columns []string, rows [][]string) []int {
	widths := make([]int, len(columns))
	for i, col := range columns {
		widths[i] = displayLen(col)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i >= len(widths) {
				continue
			}
			widths[i] = max(widths[i], min(displayLen(cell), 60))
		}
	}
	return widths
}

func ensureNoRowsMessageFits(widths []int) {
	if len(widths) == 0 {
		return
	}
	available := tableInnerWidth(widths)
	messageWidth := displayLen("no rows found")
	if available >= messageWidth {
		return
	}
	widths[len(widths)-1] += messageWidth - available
}

func tableInnerWidth(widths []int) int {
	if len(widths) == 0 {
		return 0
	}
	total := 0
	for _, width := range widths {
		total += width
	}
	return total + 3*(len(widths)-1)
}

func printSpanningRow(w io.Writer, text string, widths []int) {
	fmt.Fprintf(w, "| %-*s |\n", tableInnerWidth(widths), text)
}

func printBorder(w io.Writer, widths []int) {
	fmt.Fprint(w, "+")
	for _, width := range widths {
		fmt.Fprint(w, strings.Repeat("-", width+2), "+")
	}
	fmt.Fprintln(w)
}

func printRow(w io.Writer, row []string, widths []int) {
	fmt.Fprint(w, "|")
	for i, width := range widths {
		cell := ""
		if i < len(row) {
			cell = truncate(row[i], 60)
		}
		fmt.Fprintf(w, " %-*s |", width, cell)
	}
	fmt.Fprintln(w)
}

func printDataRow(w io.Writer, row []string, widths []int) {
	fmt.Fprint(w, "|")
	for i, width := range widths {
		cell := ""
		if i < len(row) {
			cell = truncate(row[i], width)
		}
		if isNumber(cell) {
			fmt.Fprintf(w, " %*s |", width, cell)
		} else {
			fmt.Fprintf(w, " %-*s |", width, cell)
		}
	}
	fmt.Fprintln(w)
}

func isNumber(s string) bool {
	if s == "" {
		return false
	}
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func displayLen(s string) int {
	return len([]rune(s))
}

func truncate(s string, limit int) string {
	r := []rune(s)
	if len(r) <= limit {
		return s
	}
	if limit <= 3 {
		return string(r[:limit])
	}
	return string(r[:limit-3]) + "..."
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func sortedKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
