package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// jtv serve.
//
// Every jtv run starts a process and opens DuckDB, about 80-130ms before
// the query itself, which takes a few milliseconds on a cached file.
// `jtv serve -f FILE` keeps a background process with FILE's dataset open
// and listens on a Unix socket in the cache directory. A later
// `jtv -f FILE -q ...` finds the socket, sends the query and streams the
// answer back, so only the small client process starts. It falls back to
// running locally whenever the server is missing, cannot serve the request
// (the file changed, another delimiter) or fails before writing output.
// The server exits after an idle timeout and on `jtv stop -f FILE`.

const (
	serveProcessEnv = "JTV_INTERNAL_SERVE"
	serveDelimEnv   = "JTV_INTERNAL_SERVE_DELIMITER"
	serveIdleEnv    = "JTV_INTERNAL_SERVE_IDLE"
	serveHeaderEnv  = "JTV_INTERNAL_SERVE_HEADER"

	defaultServeIdle = 30 * time.Minute
	serveStartWait   = 10 * time.Minute

	frameData     = 'D'
	frameError    = 'E'
	frameFallback = 'F'
	frameDone     = 'Z'
)

type serveRequest struct {
	Op        string `json:"op"` // query, ping or stop
	Path      string `json:"path"`
	Size      int64  `json:"size"`
	ModTime   int64  `json:"mtime"`
	Delimiter string `json:"delimiter"`
	Header    string `json:"header"`
	Query     string `json:"query"`
	Output    string `json:"output"`
	CSV       bool   `json:"csv"`
	JSON      bool   `json:"json"`
	MD        bool   `json:"md"`
}

func isServeProcess() bool {
	return os.Getenv(serveProcessEnv) != ""
}

// serveSocketPath is the socket of the server for a file.
func serveSocketPath(absPath string) string {
	dir, ok := csvCacheDir()
	if !ok {
		dir = filepath.Join(os.TempDir(), "jtv")
	}
	return filepath.Join(dir, "serve-"+shortHash(absPath)+".sock")
}

func fileIdentity(path string) (absPath string, size, modTime int64, err error) {
	absPath, err = filepath.Abs(path)
	if err != nil {
		return "", 0, 0, err
	}
	info, err := os.Stat(absPath)
	if err != nil {
		return "", 0, 0, err
	}
	return absPath, info.Size(), info.ModTime().UnixNano(), nil
}

// --- frames ---

func writeFrame(w io.Writer, kind byte, payload []byte) error {
	var header [5]byte
	header[0] = kind
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func readFrame(r io.Reader) (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, nil, err
	}
	payload := make([]byte, binary.BigEndian.Uint32(header[1:]))
	_, err := io.ReadFull(r, payload)
	return header[0], payload, err
}

// frameWriter sends everything written to it as data frames.
type frameWriter struct {
	w io.Writer
}

func (f frameWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := writeFrame(f.w, frameData, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// --- client ---

// tryServer sends a -q query to a running server for the input file. It
// reports handled=false, and the caller runs the query itself, when no
// server can answer it.
func tryServer(opts options, stdout io.Writer) (handled bool, err error) {
	if opts.query == "" || opts.file == "" || opts.file == "-" || opts.stream || opts.root != "" ||
		opts.showRequest || opts.debugRequest || opts.saveResponse != "" {
		return false, nil
	}
	if _, isURL := directURLRequest(opts.file); isURL {
		return false, nil
	}
	absPath, size, modTime, err := fileIdentity(opts.file)
	if err != nil {
		return false, nil
	}
	socket := serveSocketPath(absPath)
	if _, err := os.Stat(socket); err != nil {
		return false, nil
	}
	conn, err := net.DialTimeout("unix", socket, 200*time.Millisecond)
	if err != nil {
		return false, nil
	}
	defer conn.Close()

	output := opts.output
	if output != "" {
		if output, err = filepath.Abs(output); err != nil {
			return false, nil
		}
	}
	request := serveRequest{
		Op: "query", Path: absPath, Size: size, ModTime: modTime, Delimiter: opts.delimiter, Header: csvHeaderMode,
		Query: opts.query, Output: output, CSV: opts.csv, JSON: opts.json, MD: opts.md,
	}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return false, nil
	}
	reader := bufio.NewReaderSize(conn, 256<<10)
	wrote := false
	for {
		kind, payload, err := readFrame(reader)
		if err != nil {
			if !wrote {
				return false, nil // server went away: run locally
			}
			return true, fmt.Errorf("jtv server: %w", err)
		}
		switch kind {
		case frameData:
			wrote = true
			if _, err := stdout.Write(payload); err != nil {
				return true, err
			}
		case frameError:
			return true, errors.New(string(payload))
		case frameFallback:
			if wrote {
				return true, errors.New("jtv server: stopped while answering")
			}
			return false, nil
		case frameDone:
			return true, nil
		}
	}
}

// --- serve and stop commands ---

func runServeCommand(command string, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("jtv "+command, flag.ContinueOnError)
	fs.SetOutput(stderr)
	file := fs.String("f", "", "input file to serve")
	delimiter := fs.String("delimiter", "", "CSV delimiter: auto, comma, semicolon, pipe, tab, or one character")
	idle := fs.Duration("idle", defaultServeIdle, "stop after this long without queries; 0 keeps running")
	header := fs.String("csv-header", "auto", "CSV header row: auto, yes, or no")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *file == "" {
		return fmt.Errorf("jtv %s requires -f FILE", command)
	}
	absPath, size, modTime, err := fileIdentity(*file)
	if err != nil {
		return err
	}
	socket := serveSocketPath(absPath)

	if command == "stop" {
		if pingServer(socket, serveRequest{Op: "stop", Path: absPath}) {
			fmt.Fprintf(stdout, "stopped jtv server for %s\n", *file)
			return nil
		}
		fmt.Fprintf(stdout, "no jtv server running for %s\n", *file)
		return nil
	}

	ping := serveRequest{Op: "ping", Path: absPath, Size: size, ModTime: modTime, Delimiter: *delimiter, Header: *header}
	if pingServer(socket, ping) {
		fmt.Fprintf(stdout, "jtv server already running for %s\n", *file)
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(socket), 0o755); err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	logFile, err := os.OpenFile(strings.TrimSuffix(socket, ".sock")+".log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer logFile.Close()
	cmd := exec.Command(exe)
	cmd.Env = append(os.Environ(),
		serveProcessEnv+"="+absPath,
		serveDelimEnv+"="+*delimiter,
		serveIdleEnv+"="+idle.String(),
		serveHeaderEnv+"="+*header,
	)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	pid := cmd.Process.Pid
	cmd.Process.Release()

	fmt.Fprintf(stderr, "loading %s...\n", *file)
	deadline := time.Now().Add(serveStartWait)
	for time.Now().Before(deadline) {
		if pingServer(socket, ping) {
			idleText := idle.String()
			if *idle == 0 {
				idleText = "none"
			}
			fmt.Fprintf(stdout, "jtv server running for %s (pid %d, idle timeout %s)\nstop it with: jtv stop -f %s\n", *file, pid, idleText, *file)
			return nil
		}
		if !processAlive(pid) {
			return fmt.Errorf("jtv server exited while loading; see %s", logFile.Name())
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("jtv server did not start within %s; see %s", serveStartWait, logFile.Name())
}

func processAlive(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return process.Signal(syscall.Signal(0)) == nil
}

// pingServer sends a ping or stop request and reports whether the server
// answered it.
func pingServer(socket string, request serveRequest) bool {
	conn, err := net.DialTimeout("unix", socket, 200*time.Millisecond)
	if err != nil {
		return false
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return false
	}
	kind, _, err := readFrame(conn)
	return err == nil && kind == frameDone
}

// --- server process ---

func runServeProcess() int {
	absPath := os.Getenv(serveProcessEnv)
	delimiter := os.Getenv(serveDelimEnv)
	idle, err := time.ParseDuration(os.Getenv(serveIdleEnv))
	if err != nil {
		idle = defaultServeIdle
	}
	if mode := os.Getenv(serveHeaderEnv); validHeaderMode(mode) == nil {
		csvHeaderMode = mode
	}
	_, size, modTime, err := fileIdentity(absPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "jtv serve:", err)
		return 1
	}
	opts := options{file: absPath, delimiter: delimiter}
	loaded, err := readInput(nil, nil, opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "jtv serve:", err)
		return 1
	}
	ds, err := NewDatasetFromInput(loaded.data, loaded.path, absPath, delimiter)
	if err != nil {
		fmt.Fprintln(os.Stderr, "jtv serve:", err)
		return 1
	}
	defer ds.Close()
	loaded.data = nil
	ds.useSavedColumnNames(absPath)

	socket := serveSocketPath(absPath)
	os.Remove(socket)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		fmt.Fprintln(os.Stderr, "jtv serve:", err)
		return 1
	}
	defer os.Remove(socket)
	defer listener.Close()
	os.Chmod(socket, 0o600)
	unixListener := listener.(*net.UnixListener)

	for {
		if idle > 0 {
			unixListener.SetDeadline(time.Now().Add(idle))
		}
		conn, err := unixListener.Accept()
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				return 0 // idle timeout
			}
			fmt.Fprintln(os.Stderr, "jtv serve:", err)
			return 1
		}
		if stop := serveConnection(conn, ds, absPath, size, modTime, delimiter); stop {
			return 0
		}
	}
}

// serveConnection answers one request and reports whether to stop.
func serveConnection(conn net.Conn, ds *Dataset, absPath string, size, modTime int64, delimiter string) (stop bool) {
	defer conn.Close()
	var request serveRequest
	if err := json.NewDecoder(conn).Decode(&request); err != nil {
		return false
	}
	writer := bufio.NewWriterSize(conn, 256<<10)
	defer writer.Flush()

	// A changed file means stale data: stop so the next serve reloads it.
	if _, curSize, curModTime, err := fileIdentity(absPath); err != nil || curSize != size || curModTime != modTime {
		writeFrame(writer, frameFallback, nil)
		return true
	}
	switch request.Op {
	case "stop":
		writeFrame(writer, frameDone, nil)
		return true
	case "ping":
		if request.Size != size || request.ModTime != modTime || request.Delimiter != delimiter || request.Header != csvHeaderMode {
			writeFrame(writer, frameFallback, nil)
			return false
		}
		writeFrame(writer, frameDone, nil)
		return false
	}
	if request.Path != absPath || request.Size != size || request.ModTime != modTime || request.Delimiter != delimiter || request.Header != csvHeaderMode {
		writeFrame(writer, frameFallback, nil)
		return false
	}
	// Names may have been renamed since the server loaded the file.
	ds.refreshSavedColumnNames()
	opts := options{
		file: absPath, delimiter: delimiter, query: request.Query, output: request.Output,
		csv: request.CSV, json: request.JSON, md: request.MD,
	}
	if err := executeQuery(ds, opts, frameWriter{writer}); err != nil {
		writeFrame(writer, frameError, []byte(err.Error()))
		return false
	}
	writeFrame(writer, frameDone, nil)
	return false
}
