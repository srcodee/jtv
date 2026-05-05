package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAppendHistorySkipsEmptyAndConsecutiveDuplicate(t *testing.T) {
	history := appendHistory(nil, "")
	history = appendHistory(history, "select *")
	history = appendHistory(history, "select *")
	history = appendHistory(history, "select id")

	if len(history) != 2 {
		t.Fatalf("history length = %d, want 2", len(history))
	}
	if history[0] != "select *" || history[1] != "select id" {
		t.Fatalf("history = %v", history)
	}
}

func TestSplitCommand(t *testing.T) {
	command, arg := splitCommand("export hasil.csv")
	if command != "export" || arg != "hasil.csv" {
		t.Fatalf("command=%q arg=%q, want export hasil.csv", command, arg)
	}

	command, arg = splitCommand(".csv reports/out.csv")
	if command != ".csv" || arg != "reports/out.csv" {
		t.Fatalf("command=%q arg=%q, want .csv reports/out.csv", command, arg)
	}
}

func TestParseLSArgs(t *testing.T) {
	filter, limit := parseLSArgs("orders users -n 20")
	if filter != "orders users" || limit != 20 {
		t.Fatalf("filter=%q limit=%d, want orders users 20", filter, limit)
	}
}

func TestApplyInteractiveDefaults(t *testing.T) {
	if got := applyInteractiveDefaults("select *;"); got != "select * limit 10;" {
		t.Fatalf("got %q, want select * limit 10;", got)
	}
	if got := applyInteractiveDefaults("select id"); got != "select id" {
		t.Fatalf("got %q, want select id", got)
	}
}

func TestPrepareInteractiveQueryAddsPagination(t *testing.T) {
	query, paged := prepareInteractiveQuery("select *;", 10)
	if !paged {
		t.Fatal("expected paged query")
	}
	if query != "select * limit 10 offset 0;" {
		t.Fatalf("query = %q, want select * limit 10 offset 0;", query)
	}

	query, paged = prepareInteractiveQuery("select * limit 5", 10)
	if paged {
		t.Fatal("expected explicit limit to disable pagination")
	}
	if query != "select * limit 5" {
		t.Fatalf("query = %q, want original query", query)
	}
}

func TestAppendLimitOffset(t *testing.T) {
	got := appendLimitOffset("select id;", 20, 40)
	want := "select id limit 20 offset 40;"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestPrintPagedTableHidesFooterForSinglePage(t *testing.T) {
	result := &QueryResult{
		Columns: []string{"total"},
		Rows:    [][]string{{"340"}},
	}
	pager := pageState{page: 1, size: 10, totalRows: 1, fileRows: 30}
	var out bytes.Buffer

	printPagedTable(&out, result, pager)

	if strings.Contains(out.String(), "pagination:") || strings.Contains(out.String(), "page 1/1") {
		t.Fatalf("unexpected pagination footer: %s", out.String())
	}
}

func TestPrintPagedTableShowsFooterForMultiplePages(t *testing.T) {
	result := &QueryResult{
		Columns: []string{"id"},
		Rows:    [][]string{{"1"}, {"2"}},
	}
	pager := pageState{page: 1, size: 2, totalRows: 3, fileRows: 3}
	var out bytes.Buffer

	printPagedTable(&out, result, pager)

	if !strings.Contains(out.String(), "page 1/2") || !strings.Contains(out.String(), "pagination:") {
		t.Fatalf("missing pagination footer: %s", out.String())
	}
}

func TestPrintTableShowsNoRowsFound(t *testing.T) {
	result := &QueryResult{
		Columns: []string{"ok"},
	}
	var out bytes.Buffer

	printTable(&out, result)

	if !strings.Contains(out.String(), "no rows found") {
		t.Fatalf("missing no rows message: %s", out.String())
	}
	if strings.Contains(out.String(), "+----+\n| ok |\n+----+\n| no rows found |") {
		t.Fatalf("table is not aligned: %s", out.String())
	}
}

func TestPrintTableShowsNoRowsFoundAcrossMultipleColumns(t *testing.T) {
	result := &QueryResult{
		Columns: []string{"fulname", "username"},
	}
	var out bytes.Buffer

	printTable(&out, result)

	want := "| no rows found      |"
	if !strings.Contains(out.String(), want) {
		t.Fatalf("missing spanning no rows message %q in: %s", want, out.String())
	}
	if strings.Contains(out.String(), "| no rows found |          |") {
		t.Fatalf("table is not aligned: %s", out.String())
	}
}

func TestParseConfig(t *testing.T) {
	cfg, err := parseConfig([]byte(`
# jtv defaults
output = "json"
pagesize = 25
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.output != "json" || cfg.pageSize != 25 {
		t.Fatalf("cfg = %#v, want json pagesize 25", cfg)
	}
}

func TestRunUsesConfigOutput(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(configPath, []byte("output = \"csv\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	stdin := strings.NewReader(`[{"id":1},{"id":2}]`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-config", configPath, "-f", "-", "-q", "select id"}, stdin, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	if stdout.String() != "id\n1\n2\n" {
		t.Fatalf("stdout = %q, want csv output", stdout.String())
	}
}

func TestRunOutputFlagOverridesConfig(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(configPath, []byte("output = \"json\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	stdin := strings.NewReader(`[{"id":1}]`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-config", configPath, "-f", "-", "--csv", "-q", "select id"}, stdin, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	if stdout.String() != "id\n1\n" {
		t.Fatalf("stdout = %q, want csv output", stdout.String())
	}
}

func TestReadmeSampleCommands(t *testing.T) {
	commands := []string{
		`./jtv -f examples/users.json -q "select id, user.name, status"`,
		`./jtv -f examples/users.csv -q "select status, count(*) group by status"`,
		`./jtv -f examples/orders.json -q "select id, orders.users.id"`,
	}
	for _, command := range commands {
		t.Run(command, func(t *testing.T) {
			args := mustReadmeArgs(t, command)
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			if err := run(args, strings.NewReader(""), &stdout, &stderr); err != nil {
				t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
			}
			if stdout.Len() == 0 {
				t.Fatal("expected output")
			}
		})
	}
}

func mustReadmeArgs(t *testing.T, command string) []string {
	t.Helper()
	args, err := splitShellFields(command)
	if err != nil {
		t.Fatal(err)
	}
	if len(args) == 0 || args[0] != "./jtv" {
		t.Fatalf("unexpected README command: %s", command)
	}
	args = args[1:]
	for i, arg := range args {
		if strings.HasPrefix(arg, "examples/") {
			args[i] = filepath.Join("..", "..", arg)
		}
	}
	return args
}

func splitShellFields(line string) ([]string, error) {
	var fields []string
	var b strings.Builder
	quote := rune(0)
	escaped := false
	for _, r := range line {
		if escaped {
			b.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		if quote != 0 {
			if r == quote {
				quote = 0
			} else {
				b.WriteRune(r)
			}
			continue
		}
		if r == '\'' || r == '"' {
			quote = r
			continue
		}
		if r == ' ' || r == '\t' {
			if b.Len() > 0 {
				fields = append(fields, b.String())
				b.Reset()
			}
			continue
		}
		b.WriteRune(r)
	}
	if escaped {
		b.WriteRune('\\')
	}
	if quote != 0 {
		return nil, os.ErrInvalid
	}
	if b.Len() > 0 {
		fields = append(fields, b.String())
	}
	return fields, nil
}
