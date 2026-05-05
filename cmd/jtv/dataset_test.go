package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestQueryJSONArrayWithNestedFields(t *testing.T) {
	data := []byte(`[
		{"id":1,"user":{"name":"Ana","active":true},"order":{"total":20}},
		{"id":2,"user":{"name":"Budi","active":false},"order":{"total":15}},
		{"id":3,"user":{"name":"Ana","active":true},"order":{"total":10}}
	]`)

	ds, err := NewDataset(data, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	result, err := ds.Query(context.Background(), "select user.name, count(*) as total_count, sum(order.total) as total from input group by user.name order by total desc")
	if err != nil {
		t.Fatal(err)
	}

	wantCols := []string{"user.name", "total_count", "total"}
	if strings.Join(result.Columns, ",") != strings.Join(wantCols, ",") {
		t.Fatalf("columns = %v, want %v", result.Columns, wantCols)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(result.Rows))
	}
	if got := strings.Join(result.Rows[0], ","); got != "Ana,2,30" {
		t.Fatalf("first row = %q, want Ana,2,30", got)
	}
}

func TestQueryNDJSON(t *testing.T) {
	data := []byte(`{"id":1,"status":"ok"}
{"id":2,"status":"fail"}
{"id":3,"status":"ok"}`)

	ds, err := NewDataset(data, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	result, err := ds.Query(context.Background(), "select status, count(*) as total from input group by status order by status")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(result.Rows))
	}
	if got := strings.Join(result.Rows[1], ","); got != "ok,2" {
		t.Fatalf("second row = %q, want ok,2", got)
	}
}

func TestRunCSVToStdout(t *testing.T) {
	stdin := strings.NewReader(`[{"user":{"name":"Ana"}},{"user":{"name":"Budi"}}]`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-f", "-", "--csv", "-q", "select user.name from input order by user.name"}, stdin, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}

	want := "user.name\nAna\nBudi\n"
	if stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
}

func TestQueryCSVInput(t *testing.T) {
	data := []byte(`id,status,user.name,user.active
1,ok,Ana,1
2,ok,Budi,0
3,fail,Ana,1`)

	ds, err := NewDataset(data, "test.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	result, err := ds.Query(context.Background(), "select user.name, count(*) as total where user.active = 1 group by user.name")
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(result.Rows))
	}
	if got := strings.Join(result.Rows[0], ","); got != "Ana,2" {
		t.Fatalf("row = %q, want Ana,2", got)
	}
}

func TestRunJSONToStdout(t *testing.T) {
	stdin := strings.NewReader(`[{"id":1,"user":{"name":"Ana"}}]`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-f", "-", "--json", "-q", "select id, user.name"}, stdin, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}

	var rows []map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0]["user.name"] != "Ana" {
		t.Fatalf("rows = %#v, want user.name Ana", rows)
	}
}

func TestRunRequiresFileFlag(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-q", "select * from input"}, os.Stdin, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected missing -f error")
	}
	if !strings.Contains(err.Error(), "missing required -f") {
		t.Fatalf("error = %q, want missing required -f", err.Error())
	}
}

func TestRunReadsPipedStdinWithoutFileFlag(t *testing.T) {
	stdin := strings.NewReader(`[{"id":1},{"id":2}]`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"--csv", "-q", "select id"}, stdin, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}

	want := "id\n1\n2\n"
	if stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
}

func TestRunStreamCSV(t *testing.T) {
	stdin := strings.NewReader(`{"time":"t1","status":200,"duration_s":1.2}
{"time":"t2","status":500,"duration_s":2.5}`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"--stream", "--csv", "-q", "select time, status, duration_s"}, stdin, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}

	want := "time,status,duration_s\nt1,200,1.2\nt2,500,2.5\n"
	if stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
}

func TestRunStreamJSONLines(t *testing.T) {
	stdin := strings.NewReader(`{"time":"t1","status":200}
{"time":"t2","status":500}`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"--stream", "--json", "-q", "select time, status"}, stdin, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines = %d, want 2: %s", len(lines), stdout.String())
	}
	if !strings.Contains(lines[0], `"time":"t1"`) || !strings.Contains(lines[1], `"status":500`) {
		t.Fatalf("unexpected json lines: %s", stdout.String())
	}
}

func TestRunRejectsPositionalInputFile(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"data.json"}, strings.NewReader(`[]`), &stdout, &stderr)
	if err == nil {
		t.Fatal("expected positional input error")
	}
	if !strings.Contains(err.Error(), "input file must use -f") {
		t.Fatalf("error = %q, want input file must use -f", err.Error())
	}
}

func TestSelectStarUsesImplicitInputAndHidesRaw(t *testing.T) {
	data := []byte(`[{"id":1,"user":{"name":"Ana"}}]`)

	ds, err := NewDataset(data, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	result, err := ds.Query(context.Background(), "select *")
	if err != nil {
		t.Fatal(err)
	}

	got := strings.Join(result.Columns, ",")
	if got != "id,user.name" {
		t.Fatalf("columns = %q, want id,user.name", got)
	}
}

func TestSelectWithoutFromSupportsWhereGroupOrder(t *testing.T) {
	data := []byte(`[
		{"status":"ok","user":{"name":"Ana"}},
		{"status":"fail","user":{"name":"Ana"}},
		{"status":"ok","user":{"name":"Budi"}}
	]`)

	ds, err := NewDataset(data, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	result, err := ds.Query(context.Background(), "select user.name, count(*) as total where status = 'ok' group by user.name order by total desc")
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(result.Rows))
	}
	if got := strings.Join(result.Rows[0], ","); got != "Ana,1" && got != "Budi,1" {
		t.Fatalf("first row = %q, want one grouped row", got)
	}
}

func TestSelectStarWithoutFromSupportsWhere(t *testing.T) {
	data := []byte(`[
		{"id":1,"user":{"name":"Ana","active":true}},
		{"id":2,"user":{"name":"Budi","active":false}}
	]`)

	ds, err := NewDataset(data, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	result, err := ds.Query(context.Background(), "select * where user.active = 1;")
	if err != nil {
		t.Fatal(err)
	}

	if got := strings.Join(result.Columns, ","); got != "id,user.active,user.name" {
		t.Fatalf("columns = %q, want id,user.active,user.name", got)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(result.Rows))
	}
	if got := strings.Join(result.Rows[0], ","); got != "1,1,Ana" {
		t.Fatalf("row = %q, want 1,1,Ana", got)
	}
}

func TestNestedArraysAutoExpandWithSimpleDotPath(t *testing.T) {
	data := []byte(`[
		{
			"id": 1,
			"orders": [
				{"users": [{"id": 10}, {"id": 11}]},
				{"users": [{"id": 12}]}
			]
		}
	]`)

	ds, err := NewDataset(data, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	if ds.FieldLabels["orders.users.id"] != "orders[].users[].id" {
		t.Fatalf("label = %q, want orders[].users[].id", ds.FieldLabels["orders.users.id"])
	}

	result, err := ds.Query(context.Background(), "select id, orders.users.id")
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(result.Rows))
	}
	got := []string{
		strings.Join(result.Rows[0], ","),
		strings.Join(result.Rows[1], ","),
		strings.Join(result.Rows[2], ","),
	}
	want := []string{"1,10", "1,11", "1,12"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("rows = %v, want %v", got, want)
	}
}

func TestSelectNestedArrayFieldWithImplicitLimit(t *testing.T) {
	data := []byte(`{
		"comments": [
			{"id": 1, "body": "first"},
			{"id": 2, "body": "second"}
		],
		"total": 2,
		"skip": 0,
		"limit": 30
	}`)

	ds, err := NewDataset(data, "comments")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	result, err := ds.Query(context.Background(), "select comments.body limit 1;")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(result.Rows))
	}
	if got := strings.Join(result.Rows[0], ","); got != "first" {
		t.Fatalf("row = %q, want first", got)
	}
}

func TestSelectStarQuotesKeywordFields(t *testing.T) {
	data := []byte(`[{"id":1,"limit":30}]`)

	ds, err := NewDataset(data, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	result, err := ds.Query(context.Background(), "select *")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(result.Columns, ","); got != "id,limit" {
		t.Fatalf("columns = %q, want id,limit", got)
	}
	if got := strings.Join(result.Rows[0], ","); got != "1,30" {
		t.Fatalf("row = %q, want 1,30", got)
	}
}

func TestScalarTopLevelFieldCollapsesExpandedRows(t *testing.T) {
	data := []byte(`{
		"comments": [
			{"id": 1, "body": "first"},
			{"id": 2, "body": "second"}
		],
		"total": 340,
		"skip": 0
	}`)

	ds, err := NewDataset(data, "comments")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	result, err := ds.Query(context.Background(), "select total;")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(result.Rows))
	}
	if got := strings.Join(result.Rows[0], ","); got != "340" {
		t.Fatalf("row = %q, want 340", got)
	}

	result, err = ds.Query(context.Background(), "select total, comments.id;")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(result.Rows))
	}
}

func TestQuerySuggestsClosestField(t *testing.T) {
	data := []byte(`[{"user":{"name":"Ana"},"status":"ok"}]`)

	ds, err := NewDataset(data, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	_, err = ds.Query(context.Background(), "select user.nmae")
	if err == nil {
		t.Fatal("expected unknown field error")
	}
	if !strings.Contains(err.Error(), "did you mean user.name?") {
		t.Fatalf("error = %q, want user.name suggestion", err.Error())
	}
}
