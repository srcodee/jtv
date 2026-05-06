package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
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

[headers.example.test]
Authorization = "Bearer token"
X-Test = "from-config"
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.output != "json" || cfg.pageSize != 25 {
		t.Fatalf("cfg = %#v, want json pagesize 25", cfg)
	}
	if cfg.headers["example.test"]["Authorization"] != "Bearer token" ||
		cfg.headers["example.test"]["X-Test"] != "from-config" {
		t.Fatalf("headers = %#v, want example.test headers", cfg.headers)
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

func TestRunErrorsWhenExplicitConfigIsMissing(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-config", filepath.Join(t.TempDir(), "missing.toml"), "-f", "-", "-q", "select id"}, strings.NewReader(`[{"id":1}]`), &stdout, &stderr)
	if err == nil {
		t.Fatal("expected missing explicit config error")
	}
	if !strings.Contains(err.Error(), "missing.toml") {
		t.Fatalf("error = %q, want missing config path", err.Error())
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

func TestRunVersion(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"--version"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	if stdout.String() != "jtv 0.1.4\n" {
		t.Fatalf("stdout = %q, want version", stdout.String())
	}
}

func TestRunHelp(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-h"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	help := stderr.String()
	for _, want := range []string{
		"jtv 0.1.4",
		"Usage:",
		"jtv --version",
		"Input:",
		"request files: curl, PowerShell Invoke-WebRequest, or raw HTTP",
		"Query examples:",
		"Inspection commands:",
		`max(rows.price) as x where money(rows.price) > 10000`,
		"Numeric helpers:",
		"sum FIELD, max FIELD, min FIELD, avg FIELD",
		"Interactive commands:",
	} {
		if !strings.Contains(help, want) {
			t.Fatalf("help = %q, want %q", help, want)
		}
	}
}

func TestAggregateShortcutQuery(t *testing.T) {
	query, err := aggregateShortcutQuery("sum", "rows.price where int(rows.price) > 10000")
	if err != nil {
		t.Fatal(err)
	}
	want := "select sum(number(rows.price)) as sum where int(rows.price) > 10000"
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

func TestAggregateFunctionShortcutQuery(t *testing.T) {
	query, ok, err := expandAggregateShortcutLine("max(rows.price) as x where money(rows.price) > 10000")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected aggregate function shortcut")
	}
	want := "select max(number(rows.price)) as x where money(rows.price) > 10000"
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

func TestRunAggregateShortcut(t *testing.T) {
	stdin := strings.NewReader(`[
		{"price":"Rp 10.000"},
		{"price":"Rp 20.000"},
		{"price":"Rp 30.000"}
	]`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-f", "-", "-q", "sum price"}, stdin, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "60000") {
		t.Fatalf("stdout = %q, want sum 60000", stdout.String())
	}
}

func TestRunAggregateShortcutHonorsCSVOutput(t *testing.T) {
	stdin := strings.NewReader(`[{"price":"Rp 10.000"},{"price":"Rp 20.000"}]`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-f", "-", "--csv", "-q", "max price"}, stdin, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	if stdout.String() != "max\n20000\n" {
		t.Fatalf("stdout = %q, want CSV max", stdout.String())
	}
}

func TestRunAggregateFunctionShortcut(t *testing.T) {
	stdin := strings.NewReader(`[{"price":"Rp 10.000"},{"price":"Rp 20.000"}]`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-f", "-", "-q", "max(price) as x where money(price) > 10000"}, stdin, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "x") || !strings.Contains(stdout.String(), "20000") {
		t.Fatalf("stdout = %q, want alias x and max 20000", stdout.String())
	}
}

func TestRunQueryCommandSchema(t *testing.T) {
	stdin := strings.NewReader(`[{"id":1,"user":{"name":"Ana"}}]`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-f", "-", "-q", "ls"}, stdin, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	for _, want := range []string{"user.name", "string", "Ana"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout = %q, want %q", stdout.String(), want)
		}
	}
}

func TestRunQueryCommandDotSchema(t *testing.T) {
	stdin := strings.NewReader(`[{"id":1,"user":{"name":"Ana"}}]`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-f", "-", "-q", ".schema"}, stdin, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "user.name") {
		t.Fatalf("stdout = %q, want user.name field", stdout.String())
	}
}

func TestRunQueryCommandPreview(t *testing.T) {
	stdin := strings.NewReader(`[{"id":1},{"id":2}]`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-f", "-", "-q", "preview 1"}, stdin, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "1 row(s)") {
		t.Fatalf("stdout = %q, want one preview row", stdout.String())
	}
}

func TestRunFileCurlBashRequest(t *testing.T) {
	server := requestEchoServer(t)
	path := writeTempFile(t, `curl '`+server.URL+`/api/user' \
  -H 'Accept: application/json' \
  -H 'X-Test: bash' \
  -b 'session=abc; theme=dark'`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-f", path, "-q", "select ok, source, cookie_session"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	got := stdout.String()
	if !strings.Contains(got, "1") || !strings.Contains(got, "bash") || !strings.Contains(got, "abc") {
		t.Fatalf("stdout = %q, want bash request echo", got)
	}
}

func TestRunFileCurlWindowsRequest(t *testing.T) {
	server := requestEchoServer(t)
	path := writeTempFile(t, `curl ^"`+server.URL+`/api/user^" ^
  -H ^"Accept: application/json^" ^
  -H ^"X-Test: cmd^" ^
  -b ^"session=abc^%^3D; token=a^$b^"`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-f", path, "-q", "select source, cookie_session, cookie_token"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	got := stdout.String()
	if !strings.Contains(got, "cmd") || !strings.Contains(got, "abc%3D") || !strings.Contains(got, "a$b") {
		t.Fatalf("stdout = %q, want cmd request echo", got)
	}
}

func TestRunFilePowerShellRequest(t *testing.T) {
	server := requestEchoServer(t)
	path := writeTempFile(t, `$session = New-Object Microsoft.PowerShell.Commands.WebRequestSession
$session.UserAgent = "test-agent"
$session.Cookies.Add((New-Object System.Net.Cookie("session", "ps-cookie", "/", ".example.test")))
Invoke-WebRequest -UseBasicParsing -Uri "`+server.URL+`/api/user" `+"`"+`
-WebSession $session `+"`"+`
-Headers @{
"Accept"="application/json"
  "X-Test"="powershell"
} `+"`"+`
-ContentType "application/json"`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-f", path, "-q", "select source, cookie_session, user_agent"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	got := stdout.String()
	if !strings.Contains(got, "powershell") || !strings.Contains(got, "ps-cookie") || !strings.Contains(got, "test-agent") {
		t.Fatalf("stdout = %q, want powershell request echo", got)
	}
}

func TestRunFileRequestRejectsHTMLResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.Header.Get("Accept-Encoding"), "br") || strings.Contains(r.Header.Get("Accept-Encoding"), "zstd") {
			t.Fatalf("Accept-Encoding was forwarded: %q", r.Header.Get("Accept-Encoding"))
		}
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><title>Login</title></html>`)
	}))
	t.Cleanup(server.Close)
	path := writeTempFile(t, `curl '`+server.URL+`' -H 'Accept-Encoding: gzip, deflate, br, zstd'`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-f", path, "-q", "select *"}, strings.NewReader(""), &stdout, &stderr)
	if err == nil {
		t.Fatal("expected HTML response error")
	}
	if !strings.Contains(err.Error(), "expected JSON/NDJSON/CSV response") {
		t.Fatalf("error = %q, want response type hint", err.Error())
	}
}

func TestRunDirectURLInput(t *testing.T) {
	server := requestEchoServer(t)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-f", server.URL + "/api/user", "-q", "select ok"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "1") {
		t.Fatalf("stdout = %q, want ok row", stdout.String())
	}
}

func TestRunShowRequest(t *testing.T) {
	server := requestEchoServer(t)
	path := writeTempFile(t, `curl '`+server.URL+`/api/user' -H 'Authorization: secret' -H 'X-Test: show'`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-f", path, "--show-request"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	got := stdout.String()
	if !strings.Contains(got, "GET "+server.URL+"/api/user") ||
		!strings.Contains(got, "Authorization(redacted)") ||
		strings.Contains(got, "secret") {
		t.Fatalf("stdout = %q, want safe request summary", got)
	}
}

func TestRunDebugRequestAndSaveResponse(t *testing.T) {
	server := requestEchoServer(t)
	savePath := filepath.Join(t.TempDir(), "response.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-f", server.URL + "/api/user", "--debug-request", "--save-response", savePath, "-q", "select ok"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	debug := stderr.String()
	if !strings.Contains(debug, "response: 200 OK") || !strings.Contains(debug, "content-type: application/json") {
		t.Fatalf("stderr = %q, want response debug info", debug)
	}
	saved, err := os.ReadFile(savePath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(saved), `"ok":true`) {
		t.Fatalf("saved response = %q, want JSON body", string(saved))
	}
}

func TestRunRootOption(t *testing.T) {
	stdin := strings.NewReader(`{"data":[{"id":1},{"id":2}],"meta":{"total":2}}`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-f", "-", "--root", "data", "-q", "select id"}, stdin, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "1") || !strings.Contains(stdout.String(), "2") {
		t.Fatalf("stdout = %q, want rooted rows", stdout.String())
	}
}

func TestRunConfigHeadersForURL(t *testing.T) {
	server := requestEchoServer(t)
	u := strings.TrimPrefix(server.URL, "http://")
	host := strings.Split(u, ":")[0]
	configPath := filepath.Join(t.TempDir(), "config.toml")
	config := "[headers." + host + "]\nX-Test = \"from-config\"\n"
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-config", configPath, "-f", server.URL + "/api/user", "-q", "select source"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "from-config") {
		t.Fatalf("stdout = %q, want configured header echo", stdout.String())
	}
}

func TestRunDelimiterFlag(t *testing.T) {
	stdin := strings.NewReader("id|name\n1|Ana\n")
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-f", "-", "--delimiter", "|", "-q", "select name"}, stdin, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Ana") {
		t.Fatalf("stdout = %q, want Ana", stdout.String())
	}
}

func TestRunFileRawHTTPRequest(t *testing.T) {
	server := requestEchoServer(t)
	host := strings.TrimPrefix(server.URL, "http://")
	path := writeTempFile(t, `GET /api/user HTTP/1.1
Host: `+host+`
Accept: application/json
X-Test: raw
Cookie: session=raw-cookie
`)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run([]string{"-f", path, "-q", "select source, cookie_session"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v\nstderr: %s", err, stderr.String())
	}
	got := stdout.String()
	if !strings.Contains(got, "raw") || !strings.Contains(got, "raw-cookie") {
		t.Fatalf("stdout = %q, want raw request echo", got)
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

func requestEchoServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		session := ""
		token := ""
		if cookie, err := r.Cookie("session"); err == nil {
			session = cookie.Value
		}
		if cookie, err := r.Cookie("token"); err == nil {
			token = cookie.Value
		}
		fmt.Fprintf(w, `{"ok":true,"source":%q,"cookie_session":%q,"cookie_token":%q,"user_agent":%q}`,
			r.Header.Get("X-Test"), session, token, r.UserAgent())
	}))
	t.Cleanup(server.Close)
	return server
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "request.txt")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}
