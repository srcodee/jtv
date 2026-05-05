package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestMCPInitializeAndToolsList(t *testing.T) {
	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`,
	}, "\n")
	var out bytes.Buffer

	if err := newServer().Run(context.Background(), strings.NewReader(input), &out); err != nil {
		t.Fatal(err)
	}

	responses := decodeResponses(t, out.String())
	if len(responses) != 2 {
		t.Fatalf("responses = %d, want 2: %s", len(responses), out.String())
	}
	if responses[0]["error"] != nil {
		t.Fatalf("initialize error: %#v", responses[0]["error"])
	}
	result := responses[1]["result"].(map[string]any)
	tools := result["tools"].([]any)
	if len(tools) != 8 {
		t.Fatalf("tools = %d, want 8", len(tools))
	}
}

func TestMCPQueryTool(t *testing.T) {
	input := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"jtv_query","arguments":{"data":"[{\"id\":1,\"user\":{\"name\":\"Ana\"}},{\"id\":2,\"user\":{\"name\":\"Budi\"}}]","query":"select user.name order by id"}}}`
	var out bytes.Buffer

	if err := newServer().Run(context.Background(), strings.NewReader(input), &out); err != nil {
		t.Fatal(err)
	}

	responses := decodeResponses(t, out.String())
	result := responses[0]["result"].(map[string]any)
	if result["isError"] == true {
		t.Fatalf("tool returned error: %#v", result)
	}
	structured := result["structuredContent"].(map[string]any)
	rows := structured["rows"].([]any)
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	first := rows[0].([]any)
	if first[0] != "Ana" {
		t.Fatalf("first row = %#v, want Ana", first)
	}
}

func TestMCPQueryToolURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-CSRF-Token") != "token-123" {
			http.Error(w, "missing csrf", http.StatusForbidden)
			return
		}
		if r.Header.Get("Cookie") != "session=abc" {
			http.Error(w, "missing cookie", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"id":1,"name":"Ana","score":90},{"id":2,"name":"Budi","score":75}]`))
	}))
	defer srv.Close()

	input := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"jtv_query","arguments":{"url":"` + srv.URL + `","headers":{"Cookie":"session=abc","X-CSRF-Token":"token-123"},"query":"select name, score order by score desc limit 1"}}}`
	var out bytes.Buffer

	if err := newServer().Run(context.Background(), strings.NewReader(input), &out); err != nil {
		t.Fatal(err)
	}

	responses := decodeResponses(t, out.String())
	result := responses[0]["result"].(map[string]any)
	if result["isError"] == true {
		t.Fatalf("tool returned error: %#v", result)
	}
	structured := result["structuredContent"].(map[string]any)
	objects := structured["objects"].([]any)
	first := objects[0].(map[string]any)
	if first["name"] != "Ana" || first["score"] != float64(90) {
		t.Fatalf("first object = %#v, want Ana score 90", first)
	}
}

func TestMCPQueryToolURLMethodAndBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "wrong method", http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Authorization") != "Bearer secret" {
			http.Error(w, "missing authorization", http.StatusForbidden)
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if string(raw) != `{"active":true}` {
			http.Error(w, "wrong body", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"id":1,"status":"ok"}]`))
	}))
	defer srv.Close()

	input := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"jtv_query","arguments":{"url":"` + srv.URL + `","method":"POST","headers":{"Authorization":"Bearer secret","Content-Type":"application/json"},"body":"{\"active\":true}","query":"select id, status"}}}`
	var out bytes.Buffer

	if err := newServer().Run(context.Background(), strings.NewReader(input), &out); err != nil {
		t.Fatal(err)
	}

	responses := decodeResponses(t, out.String())
	result := responses[0]["result"].(map[string]any)
	if result["isError"] == true {
		t.Fatalf("tool returned error: %#v", result)
	}
	structured := result["structuredContent"].(map[string]any)
	objects := structured["objects"].([]any)
	first := objects[0].(map[string]any)
	if first["status"] != "ok" {
		t.Fatalf("first object = %#v, want status ok", first)
	}
}

func TestMCPSchemaTool(t *testing.T) {
	input := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"jtv_schema","arguments":{"data":"[{\"id\":1,\"user\":{\"name\":\"Ana\"}}]"}}}`
	var out bytes.Buffer

	if err := newServer().Run(context.Background(), strings.NewReader(input), &out); err != nil {
		t.Fatal(err)
	}

	responses := decodeResponses(t, out.String())
	result := responses[0]["result"].(map[string]any)
	structured := result["structuredContent"].(map[string]any)
	fields := structured["fields"].([]any)
	if !containsAny(fields, "user.name") {
		t.Fatalf("fields = %#v, want user.name", fields)
	}
}

func TestMCPStreamQueryTool(t *testing.T) {
	input := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"jtv_stream_query","arguments":{"data":"{\"time\":\"t1\",\"status\":\"ok\"}\n{\"time\":\"t2\",\"status\":\"fail\"}","query":"select time, status"}}}`
	var out bytes.Buffer

	if err := newServer().Run(context.Background(), strings.NewReader(input), &out); err != nil {
		t.Fatal(err)
	}

	responses := decodeResponses(t, out.String())
	result := responses[0]["result"].(map[string]any)
	if result["isError"] == true {
		t.Fatalf("tool returned error: %#v", result)
	}
	structured := result["structuredContent"].(map[string]any)
	if structured["processed"] != float64(2) {
		t.Fatalf("processed = %#v, want 2", structured["processed"])
	}
	events := structured["events"].([]any)
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
	first := events[0].(map[string]any)
	objects := first["objects"].([]any)
	row := objects[0].(map[string]any)
	if row["status"] != "ok" {
		t.Fatalf("first object = %#v, want status ok", row)
	}
}

func TestMCPStreamSessionTools(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.ndjson")
	if err := os.WriteFile(path, []byte("{\"status\":\"ok\"}\n{\"status\":\"fail\"}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"jtv_stream_start","arguments":{"file_path":"` + path + `","query":"select status"}}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"jtv_stream_read","arguments":{"session_id":"stream-1","limit":1}}}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"jtv_stream_read","arguments":{"session_id":"stream-1"}}}`,
		`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"jtv_stream_stop","arguments":{"session_id":"stream-1"}}}`,
	}, "\n")
	var out bytes.Buffer

	if err := newServer().Run(context.Background(), strings.NewReader(input), &out); err != nil {
		t.Fatal(err)
	}

	responses := decodeResponses(t, out.String())
	if len(responses) != 4 {
		t.Fatalf("responses = %d, want 4: %s", len(responses), out.String())
	}
	firstRead := responses[1]["result"].(map[string]any)["structuredContent"].(map[string]any)
	if firstRead["processed"] != float64(1) {
		t.Fatalf("first read = %#v, want 1 processed", firstRead)
	}
	secondRead := responses[2]["result"].(map[string]any)["structuredContent"].(map[string]any)
	if secondRead["processed"] != float64(1) {
		t.Fatalf("second read = %#v, want 1 processed", secondRead)
	}
	stop := responses[3]["result"].(map[string]any)["structuredContent"].(map[string]any)
	if stop["stopped"] != true {
		t.Fatalf("stop = %#v, want stopped", stop)
	}
}

func TestMCPStdioBinaryIntegration(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "jtv-mcp")
	build := exec.Command("go", "build", "-buildvcs=false", "-o", bin, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, output)
	}

	cmd := exec.Command(bin)
	cmd.Stdin = strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}` + "\n")
	var out bytes.Buffer
	cmd.Stdout = &out
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	responses := decodeResponses(t, out.String())
	result := responses[0]["result"].(map[string]any)
	if len(result["tools"].([]any)) != 8 {
		t.Fatalf("tools result = %#v", result)
	}
}

func TestMCPExportToolCSV(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), "users.csv")
	input := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"jtv_export","arguments":{"data":"[{\"id\":1,\"user\":{\"name\":\"Ana\"}},{\"id\":2,\"user\":{\"name\":\"Budi\"}}]","query":"select id, user.name order by id","output_path":"` + outputPath + `"}}}`
	var out bytes.Buffer

	if err := newServer().Run(context.Background(), strings.NewReader(input), &out); err != nil {
		t.Fatal(err)
	}

	responses := decodeResponses(t, out.String())
	result := responses[0]["result"].(map[string]any)
	if result["isError"] == true {
		t.Fatalf("tool returned error: %#v", result)
	}
	structured := result["structuredContent"].(map[string]any)
	if structured["rows"] != float64(2) || structured["format"] != "csv" {
		t.Fatalf("structured = %#v, want csv with 2 rows", structured)
	}
	raw, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	want := "id,user.name\n1,Ana\n2,Budi\n"
	if string(raw) != want {
		t.Fatalf("export = %q, want %q", string(raw), want)
	}
}

func TestMCPExportToolJSON(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), "users.json")
	input := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"jtv_export","arguments":{"data":"[{\"id\":1,\"user\":{\"name\":\"Ana\"}}]","query":"select id, user.name","output_path":"` + outputPath + `"}}}`
	var out bytes.Buffer

	if err := newServer().Run(context.Background(), strings.NewReader(input), &out); err != nil {
		t.Fatal(err)
	}

	responses := decodeResponses(t, out.String())
	result := responses[0]["result"].(map[string]any)
	if result["isError"] == true {
		t.Fatalf("tool returned error: %#v", result)
	}
	raw, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"user.name": "Ana"`) {
		t.Fatalf("export json = %s, want user.name Ana", string(raw))
	}
}

func decodeResponses(t *testing.T, output string) []map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	responses := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		var response map[string]any
		if err := json.Unmarshal([]byte(line), &response); err != nil {
			t.Fatalf("invalid JSON response %q: %v", line, err)
		}
		responses = append(responses, response)
	}
	return responses
}

func containsAny(values []any, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
