package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/srcodee/jtv/internal/jtvcore"
	_ "modernc.org/sqlite"
)

const version = "0.1.2"

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type tool struct {
	Name        string
	Description string
	InputSchema map[string]any
	Handler     func(context.Context, map[string]any) (toolResult, error)
}

type toolResult struct {
	Content           []contentBlock `json:"content"`
	StructuredContent any            `json:"structuredContent,omitempty"`
	IsError           bool           `json:"isError,omitempty"`
}

type contentBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type server struct {
	tools   []tool
	streams map[string]*streamSession
	nextID  int
}

type streamSession struct {
	ID       string
	FilePath string
	Query    string
	Offset   int64
	Line     int
	Done     bool
}

func main() {
	if err := newServer().Run(context.Background(), os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func newServer() *server {
	s := &server{streams: map[string]*streamSession{}}
	s.tools = []tool{
		{
			Name:        "jtv_query",
			Description: "Query JSON, NDJSON, or CSV data with SQL. Provide either data or file_path.",
			InputSchema: objectSchema(map[string]any{
				"data":      stringSchema("Inline JSON, NDJSON, or CSV input."),
				"file_path": stringSchema("Local JSON, NDJSON, or CSV file path."),
				"query":     stringSchema("SQL query, for example: select user.name, count(*) group by user.name"),
			}, []string{"query"}),
			Handler: handleQuery,
		},
		{
			Name:        "jtv_schema",
			Description: "List detected flattened fields for JSON, NDJSON, or CSV data.",
			InputSchema: objectSchema(map[string]any{
				"data":      stringSchema("Inline JSON, NDJSON, or CSV input."),
				"file_path": stringSchema("Local JSON, NDJSON, or CSV file path."),
				"filter":    stringSchema("Optional substring filter for field names."),
			}, nil),
			Handler: handleSchema,
		},
		{
			Name:        "jtv_preview",
			Description: "Preview the first rows from JSON, NDJSON, or CSV data.",
			InputSchema: objectSchema(map[string]any{
				"data":      stringSchema("Inline JSON, NDJSON, or CSV input."),
				"file_path": stringSchema("Local JSON, NDJSON, or CSV file path."),
				"limit":     numberSchema("Maximum number of rows to return. Default is 10."),
			}, nil),
			Handler: handlePreview,
		},
		{
			Name:        "jtv_stream_query",
			Description: "Run a SQL query independently for each NDJSON line. Provide either data or file_path.",
			InputSchema: objectSchema(map[string]any{
				"data":      stringSchema("Inline NDJSON input. Each non-empty line is one JSON value."),
				"file_path": stringSchema("Local NDJSON file path."),
				"query":     stringSchema("SQL query to run against each line, for example: select time, status."),
				"limit":     numberSchema("Maximum number of non-empty lines to process. Default is all lines."),
			}, []string{"query"}),
			Handler: handleStreamQuery,
		},
		{
			Name:        "jtv_export",
			Description: "Query JSON, NDJSON, or CSV data and write the result to a CSV or JSON file.",
			InputSchema: objectSchema(map[string]any{
				"data":        stringSchema("Inline JSON, NDJSON, or CSV input."),
				"file_path":   stringSchema("Local JSON, NDJSON, or CSV file path."),
				"query":       stringSchema("SQL query to export."),
				"output_path": stringSchema("File path to write. .csv and .json are supported."),
				"format":      stringSchema("Optional output format: csv or json. Defaults to output_path extension."),
			}, []string{"query", "output_path"}),
			Handler: handleExport,
		},
		{
			Name:        "jtv_stream_start",
			Description: "Start a stateful NDJSON file stream session. Use jtv_stream_read to process new lines.",
			InputSchema: objectSchema(map[string]any{
				"file_path": stringSchema("Local NDJSON file path to read incrementally."),
				"query":     stringSchema("SQL query to run independently for each new line."),
			}, []string{"file_path", "query"}),
			Handler: s.handleStreamStart,
		},
		{
			Name:        "jtv_stream_read",
			Description: "Read and query new lines from a stream session created by jtv_stream_start.",
			InputSchema: objectSchema(map[string]any{
				"session_id": stringSchema("Stream session ID returned by jtv_stream_start."),
				"limit":      numberSchema("Maximum number of new non-empty lines to process. Default is all available lines."),
			}, []string{"session_id"}),
			Handler: s.handleStreamRead,
		},
		{
			Name:        "jtv_stream_stop",
			Description: "Stop and remove a stream session created by jtv_stream_start.",
			InputSchema: objectSchema(map[string]any{
				"session_id": stringSchema("Stream session ID returned by jtv_stream_start."),
			}, []string{"session_id"}),
			Handler: s.handleStreamStop,
		},
	}
	return s
}

func (s *server) Run(ctx context.Context, in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 1024*64), 1024*1024*16)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		responses := s.handleLine(ctx, []byte(line))
		for _, response := range responses {
			if err := writeJSONLine(out, response); err != nil {
				return err
			}
		}
	}
	return scanner.Err()
}

func (s *server) handleLine(ctx context.Context, data []byte) []rpcResponse {
	if len(data) > 0 && data[0] == '[' {
		var requests []rpcRequest
		if err := json.Unmarshal(data, &requests); err != nil {
			return []rpcResponse{errorResponse(nil, -32700, "parse error")}
		}
		responses := make([]rpcResponse, 0, len(requests))
		for _, request := range requests {
			if response, ok := s.handleRequest(ctx, request); ok {
				responses = append(responses, response)
			}
		}
		return responses
	}

	var request rpcRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return []rpcResponse{errorResponse(nil, -32700, "parse error")}
	}
	if response, ok := s.handleRequest(ctx, request); ok {
		return []rpcResponse{response}
	}
	return nil
}

func (s *server) handleRequest(ctx context.Context, request rpcRequest) (rpcResponse, bool) {
	if len(request.ID) == 0 {
		return rpcResponse{}, false
	}
	switch request.Method {
	case "initialize":
		return resultResponse(request.ID, map[string]any{
			"protocolVersion": requestedProtocolVersion(request.Params),
			"capabilities": map[string]any{
				"tools": map[string]any{"listChanged": false},
			},
			"serverInfo": map[string]any{
				"name":    "jtv-mcp",
				"version": version,
			},
		}), true
	case "ping":
		return resultResponse(request.ID, map[string]any{}), true
	case "tools/list":
		return resultResponse(request.ID, map[string]any{"tools": s.toolDefinitions()}), true
	case "tools/call":
		result := s.callTool(ctx, request.Params)
		return resultResponse(request.ID, result), true
	default:
		return errorResponse(request.ID, -32601, "method not found"), true
	}
}

func requestedProtocolVersion(params json.RawMessage) string {
	var payload struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if err := json.Unmarshal(params, &payload); err == nil && payload.ProtocolVersion != "" {
		return payload.ProtocolVersion
	}
	return "2025-06-18"
}

func (s *server) toolDefinitions() []map[string]any {
	out := make([]map[string]any, 0, len(s.tools))
	for _, tool := range s.tools {
		out = append(out, map[string]any{
			"name":        tool.Name,
			"description": tool.Description,
			"inputSchema": tool.InputSchema,
		})
	}
	return out
}

func (s *server) callTool(ctx context.Context, params json.RawMessage) toolResult {
	var payload struct {
		Name      string         `json:"name"`
		Arguments map[string]any `json:"arguments"`
	}
	if err := json.Unmarshal(params, &payload); err != nil {
		return errorToolResult("invalid tools/call params")
	}
	for _, tool := range s.tools {
		if tool.Name != payload.Name {
			continue
		}
		result, err := tool.Handler(ctx, payload.Arguments)
		if err != nil {
			return errorToolResult(err.Error())
		}
		return result
	}
	return errorToolResult("unknown tool: " + payload.Name)
}

func handleQuery(ctx context.Context, args map[string]any) (toolResult, error) {
	ds, err := datasetFromArgs(args)
	if err != nil {
		return toolResult{}, err
	}
	defer ds.Close()

	query := stringArg(args, "query")
	if query == "" {
		return toolResult{}, errors.New("query is required")
	}
	result, err := ds.Query(ctx, query)
	if err != nil {
		return toolResult{}, err
	}
	payload := resultPayload(result)
	return jsonToolResult(payload)
}

func handleSchema(ctx context.Context, args map[string]any) (toolResult, error) {
	_ = ctx
	ds, err := datasetFromArgs(args)
	if err != nil {
		return toolResult{}, err
	}
	defer ds.Close()

	filter := strings.ToLower(stringArg(args, "filter"))
	fields := make([]string, 0, len(ds.Fields))
	for _, field := range ds.Fields {
		if field == "raw" {
			continue
		}
		label := ds.FieldLabels[field]
		if label == "" {
			label = field
		}
		if filter != "" && !strings.Contains(strings.ToLower(field), filter) && !strings.Contains(strings.ToLower(label), filter) {
			continue
		}
		fields = append(fields, label)
	}
	return jsonToolResult(map[string]any{"fields": fields, "count": len(fields)})
}

func handlePreview(ctx context.Context, args map[string]any) (toolResult, error) {
	ds, err := datasetFromArgs(args)
	if err != nil {
		return toolResult{}, err
	}
	defer ds.Close()

	limit := intArg(args, "limit", 10)
	if limit < 1 {
		return toolResult{}, errors.New("limit must be >= 1")
	}
	result, err := ds.Query(ctx, fmt.Sprintf("select * limit %d", limit))
	if err != nil {
		return toolResult{}, err
	}
	return jsonToolResult(resultPayload(result))
}

func handleStreamQuery(ctx context.Context, args map[string]any) (toolResult, error) {
	query := stringArg(args, "query")
	if query == "" {
		return toolResult{}, errors.New("query is required")
	}
	data, source, err := inputDataFromArgs(args)
	if err != nil {
		return toolResult{}, err
	}
	limit := intArg(args, "limit", 0)
	if limit < 0 {
		return toolResult{}, errors.New("limit must be >= 0")
	}

	scanner := bufio.NewScanner(strings.NewReader(data))
	scanner.Buffer(make([]byte, 1024*64), 1024*1024*16)
	events := make([]map[string]any, 0)
	lineNumber := 0
	processed := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if limit > 0 && processed >= limit {
			break
		}
		processed++
		events = append(events, queryStreamLine(ctx, []byte(line), query, source, lineNumber))
	}
	if err := scanner.Err(); err != nil {
		return toolResult{}, err
	}

	return jsonToolResult(map[string]any{
		"events":    events,
		"processed": processed,
	})
}

func handleExport(ctx context.Context, args map[string]any) (toolResult, error) {
	ds, err := datasetFromArgs(args)
	if err != nil {
		return toolResult{}, err
	}
	defer ds.Close()

	query := stringArg(args, "query")
	if query == "" {
		return toolResult{}, errors.New("query is required")
	}
	outputPath := stringArg(args, "output_path")
	if outputPath == "" {
		return toolResult{}, errors.New("output_path is required")
	}
	format, err := exportFormat(outputPath, stringArg(args, "format"))
	if err != nil {
		return toolResult{}, err
	}

	result, err := ds.Query(ctx, query)
	if err != nil {
		return toolResult{}, err
	}
	if err := writeExportFile(outputPath, format, result); err != nil {
		return toolResult{}, err
	}
	return jsonToolResult(map[string]any{
		"output_path": outputPath,
		"format":      format,
		"rows":        len(result.Rows),
		"columns":     result.Columns,
	})
}

func (s *server) handleStreamStart(ctx context.Context, args map[string]any) (toolResult, error) {
	_ = ctx
	filePath := stringArg(args, "file_path")
	if filePath == "" {
		return toolResult{}, errors.New("file_path is required")
	}
	query := stringArg(args, "query")
	if query == "" {
		return toolResult{}, errors.New("query is required")
	}
	if _, err := os.Stat(filePath); err != nil {
		return toolResult{}, err
	}
	s.nextID++
	id := fmt.Sprintf("stream-%d", s.nextID)
	session := &streamSession{ID: id, FilePath: filePath, Query: query}
	s.streams[id] = session
	return jsonToolResult(map[string]any{
		"session_id": id,
		"file_path":  filePath,
		"query":      query,
	})
}

func (s *server) handleStreamRead(ctx context.Context, args map[string]any) (toolResult, error) {
	sessionID := stringArg(args, "session_id")
	if sessionID == "" {
		return toolResult{}, errors.New("session_id is required")
	}
	session, ok := s.streams[sessionID]
	if !ok {
		return toolResult{}, errors.New("unknown stream session: " + sessionID)
	}
	limit := intArg(args, "limit", 0)
	if limit < 0 {
		return toolResult{}, errors.New("limit must be >= 0")
	}

	events, processed, offset, err := readStreamSession(ctx, session, limit)
	if err != nil {
		return toolResult{}, err
	}
	session.Offset = offset
	session.Line += processed

	return jsonToolResult(map[string]any{
		"session_id": session.ID,
		"events":     events,
		"processed":  processed,
		"offset":     session.Offset,
		"line":       session.Line,
		"done":       session.Done,
	})
}

func (s *server) handleStreamStop(ctx context.Context, args map[string]any) (toolResult, error) {
	_ = ctx
	sessionID := stringArg(args, "session_id")
	if sessionID == "" {
		return toolResult{}, errors.New("session_id is required")
	}
	if _, ok := s.streams[sessionID]; !ok {
		return toolResult{}, errors.New("unknown stream session: " + sessionID)
	}
	delete(s.streams, sessionID)
	return jsonToolResult(map[string]any{"session_id": sessionID, "stopped": true})
}

func readStreamSession(ctx context.Context, session *streamSession, limit int) ([]map[string]any, int, int64, error) {
	file, err := os.Open(session.FilePath)
	if err != nil {
		return nil, 0, session.Offset, err
	}
	defer file.Close()

	if _, err := file.Seek(session.Offset, io.SeekStart); err != nil {
		return nil, 0, session.Offset, err
	}
	reader := bufio.NewReader(file)
	events := make([]map[string]any, 0)
	processed := 0
	offset := session.Offset
	for {
		if limit > 0 && processed >= limit {
			break
		}
		line, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, 0, offset, err
		}
		if line == "" && errors.Is(err, io.EOF) {
			break
		}
		offset += int64(len(line))
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			processed++
			event := queryStreamLine(ctx, []byte(trimmed), session.Query, session.FilePath, session.Line+processed)
			events = append(events, event)
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}
	return events, processed, offset, nil
}

func queryStreamLine(ctx context.Context, line []byte, query, source string, lineNumber int) map[string]any {
	event := map[string]any{"line": lineNumber}
	ds, err := jtvcore.NewDataset(line, fmt.Sprintf("%s:%d", source, lineNumber))
	if err != nil {
		event["error"] = err.Error()
		return event
	}
	result, err := ds.Query(ctx, query)
	ds.Close()
	if err != nil {
		event["error"] = err.Error()
		return event
	}
	event["columns"] = result.Columns
	event["rows"] = result.Rows
	event["objects"] = jtvcore.ResultRowsAsObjects(result)
	return event
}

func datasetFromArgs(args map[string]any) (*jtvcore.Dataset, error) {
	data, source, err := inputDataFromArgs(args)
	if err != nil {
		return nil, err
	}
	return jtvcore.NewDataset([]byte(data), source)
}

func inputDataFromArgs(args map[string]any) (string, string, error) {
	data := stringArg(args, "data")
	if data != "" {
		return data, "inline", nil
	}
	path := stringArg(args, "file_path")
	if path == "" {
		return "", "", errors.New("data or file_path is required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", "", err
	}
	return string(raw), path, nil
}

func resultPayload(result *jtvcore.QueryResult) map[string]any {
	return map[string]any{
		"columns": result.Columns,
		"rows":    result.Rows,
		"objects": jtvcore.ResultRowsAsObjects(result),
	}
}

func exportFormat(outputPath, requested string) (string, error) {
	format := strings.ToLower(strings.TrimSpace(requested))
	if format == "" {
		switch strings.ToLower(filepath.Ext(outputPath)) {
		case ".json":
			format = "json"
		case ".csv", "":
			format = "csv"
		default:
			return "", fmt.Errorf("unsupported output extension %q; use .csv or .json", filepath.Ext(outputPath))
		}
	}
	switch format {
	case "csv", "json":
		return format, nil
	default:
		return "", errors.New("format must be csv or json")
	}
}

func writeExportFile(path, format string, result *jtvcore.QueryResult) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	switch format {
	case "csv":
		return jtvcore.WriteCSV(file, result)
	case "json":
		return jtvcore.WriteJSON(file, result)
	default:
		return errors.New("format must be csv or json")
	}
}

func jsonToolResult(payload any) (toolResult, error) {
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return toolResult{}, err
	}
	return toolResult{
		Content:           []contentBlock{{Type: "text", Text: string(raw)}},
		StructuredContent: payload,
	}, nil
}

func errorToolResult(message string) toolResult {
	return toolResult{
		Content: []contentBlock{{Type: "text", Text: message}},
		IsError: true,
	}
}

func stringArg(args map[string]any, key string) string {
	value, ok := args[key]
	if !ok || value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	default:
		return fmt.Sprint(v)
	}
}

func intArg(args map[string]any, key string, fallback int) int {
	value, ok := args[key]
	if !ok || value == nil {
		return fallback
	}
	switch v := value.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case json.Number:
		n, err := v.Int64()
		if err == nil {
			return int(n)
		}
	case string:
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return fallback
}

func objectSchema(properties map[string]any, required []string) map[string]any {
	schema := map[string]any{
		"type":       "object",
		"properties": properties,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func stringSchema(description string) map[string]any {
	return map[string]any{"type": "string", "description": description}
}

func numberSchema(description string) map[string]any {
	return map[string]any{"type": "number", "description": description}
}

func resultResponse(id json.RawMessage, result any) rpcResponse {
	return rpcResponse{JSONRPC: "2.0", ID: id, Result: result}
}

func errorResponse(id json.RawMessage, code int, message string) rpcResponse {
	return rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: message}}
}

func writeJSONLine(out io.Writer, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, string(raw))
	return err
}
