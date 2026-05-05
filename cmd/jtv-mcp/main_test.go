package main

import (
	"bytes"
	"context"
	"encoding/json"
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
	if len(tools) != 3 {
		t.Fatalf("tools = %d, want 3", len(tools))
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
