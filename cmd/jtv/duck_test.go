package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func queryRows(t *testing.T, ds *Dataset, query string) [][]string {
	t.Helper()
	result, err := ds.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return result.Rows
}

func TestCSVColumnTypesFollowValues(t *testing.T) {
	data := []byte("phone|code|active|price|note\n" +
		"620001|6|true|1.5| a \n" +
		"628222||FALSE|2|\n" +
		"sanity01|11|true|3|x\n")

	ds, err := NewDataset(data, "test.psv")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	want := map[string]string{"phone": "string", "code": "int", "active": "bool", "price": "number", "note": "string"}
	for field, typ := range want {
		if ds.FieldTypes[field] != typ {
			t.Errorf("%s type = %q, want %q", field, ds.FieldTypes[field], typ)
		}
	}
	got := queryRows(t, ds, "select phone, code, active, price, note")
	wantRows := []string{"620001,6,1,1.5,a", "628222,,0,2,", "sanity01,11,1,3,x"}
	for i, row := range got {
		if strings.Join(row, ",") != wantRows[i] {
			t.Fatalf("row %d = %v, want %s", i, row, wantRows[i])
		}
	}
	if got := queryRows(t, ds, "select count(*) where code is null"); got[0][0] != "1" {
		t.Fatalf("null codes = %v", got)
	}
}

func TestNumericLiteralMatchesMixedTextColumn(t *testing.T) {
	data := []byte("phone|code\n620001|6\n628222|1\nsanity01|11\n")
	ds, err := NewDataset(data, "test.psv")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	if got := queryRows(t, ds, "select code where phone = 628222"); len(got) != 1 || got[0][0] != "1" {
		t.Fatalf("= literal: %v", got)
	}
	if got := queryRows(t, ds, `select count(*) where "phone" in (620001, 628222)`); got[0][0] != "2" {
		t.Fatalf("in list: %v", got)
	}
	if got := queryRows(t, ds, "select count(*) where phone <> 620001"); got[0][0] != "2" {
		t.Fatalf("<> literal: %v", got)
	}
	if got := queryRows(t, ds, "select count(*) where code = 11"); got[0][0] != "1" {
		t.Fatalf("numeric column: %v", got)
	}
}

func TestQuoteTextComparisons(t *testing.T) {
	text := map[string]bool{"phone": true}
	cases := map[string]string{
		`select * from input where "phone" = 628`:          `select * from input where "phone" = '628'`,
		`select * from input where "Phone"!=-1.5`:          `select * from input where "Phone"!='-1.5'`,
		`select * from input where "phone" not in (1, 2)`:  `select * from input where "phone" not in ('1', '2')`,
		`select * from input where "phone" in (1, "x")`:    `select * from input where "phone" in (1, "x")`,
		`select * from input where "phone" > 5`:            `select * from input where try_cast("phone" as double) > 5`,
		`select * from input where "phone" <= -1.5`:        `select * from input where try_cast("phone" as double) <= -1.5`,
		`select * from input where "phone" <> 5`:           `select * from input where "phone" <> '5'`,
		`select * from input where "code" = ''`:            `select * from input where "code" is null`,
		`select * from input where "code" != '' and 1=1`:   `select * from input where "code" is not null and 1=1`,
		`select * from input where "code" = 'x'`:           `select * from input where "code" = 'x'`,
		`select * from input where "code" = 5`:             `select * from input where "code" = 5`,
		`select * from input where "phone" = '5' or 1 = 1`: `select * from input where "phone" = '5' or 1 = 1`,
	}
	for in, want := range cases {
		if got := quoteTextComparisons(in, []string{"phone", "code"}, text); got != want {
			t.Errorf("quoteTextComparisons(%q)\n got %q\nwant %q", in, got, want)
		}
	}
}

func TestScalarQueryKeepsFirstAppearanceOrder(t *testing.T) {
	data := []byte("name,n\nBudi,1\nAna,2\nBudi,3\nCici,4\nAna,5\n")
	ds, err := NewDataset(data, "test.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	got := queryRows(t, ds, "select name")
	if fmt.Sprint(got) != "[[Budi] [Ana] [Cici]]" {
		t.Fatalf("rows = %v", got)
	}
	if got := queryRows(t, ds, "select name limit 2"); fmt.Sprint(got) != "[[Budi] [Ana]]" {
		t.Fatalf("limited rows = %v", got)
	}
}

func TestLargeCSVFileIsReadByDuckDBDirectly(t *testing.T) {
	var b bytes.Buffer
	b.WriteString("id|name\n")
	rows := 0
	for b.Len() <= peekPrefixSize*2 {
		rows++
		fmt.Fprintf(&b, "%d|user-%d\n", rows, rows)
	}
	path := filepath.Join(t.TempDir(), "big.psv")
	if err := os.WriteFile(path, b.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	loaded, ok, err := peekDelimitedFile(path, options{})
	if err != nil || !ok {
		t.Fatalf("peekDelimitedFile ok=%v err=%v", ok, err)
	}
	if len(loaded.data) != peekPrefixSize {
		t.Fatalf("peeked %d bytes, want %d", len(loaded.data), peekPrefixSize)
	}

	var stdout, stderr bytes.Buffer
	if err := run([]string{"-f", path, "--csv", "-q", "select count(*), max(id)"}, strings.NewReader(""), &stdout, &stderr); err != nil {
		t.Fatalf("run: %v\n%s", err, stderr.String())
	}
	want := fmt.Sprintf("count(*),max(id)\n%d,%d\n", rows, rows)
	if stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
}

func TestPeekLeavesJSONFilesToTheJSONLoader(t *testing.T) {
	var b bytes.Buffer
	for b.Len() <= peekPrefixSize {
		b.WriteString(`{"id":1}` + "\n")
	}
	path := filepath.Join(t.TempDir(), "big.ndjson")
	if err := os.WriteFile(path, b.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := peekDelimitedFile(path, options{}); ok || err != nil {
		t.Fatalf("peekDelimitedFile ok=%v err=%v, want JSON left to the full loader", ok, err)
	}
}

func TestCSVRawColumn(t *testing.T) {
	ds, err := NewDataset([]byte("id,name\n1,Ana\n"), "test.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	if got := queryRows(t, ds, "select raw"); got[0][0] != `{"id":1,"name":"Ana"}` {
		t.Fatalf("raw = %v", got)
	}
	if got := queryRows(t, ds, "select *"); fmt.Sprint(got) != "[[1 Ana]]" {
		t.Fatalf("select * = %v, want raw and row id hidden", got)
	}
}

func TestCSVCacheIsReusedAndRefreshedWhenFileChanges(t *testing.T) {
	cacheDir := t.TempDir()
	t.Setenv("JTV_CACHE_DIR", cacheDir)
	oldMin := csvCacheMinSize
	csvCacheMinSize = 0
	t.Cleanup(func() { csvCacheMinSize = oldMin })

	path := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(path, []byte("phone,code\n620001,6\nsanity,1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	load := func() *Dataset {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		ds, err := NewDatasetFromInput(data, path, path, "")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ds.Close() })
		return ds
	}
	caches := func() []string {
		matches, _ := filepath.Glob(filepath.Join(cacheDir, "*.duckdb"))
		return matches
	}

	first := load()
	if got := caches(); len(got) != 1 {
		t.Fatalf("caches after first load = %v, want 1", got)
	}
	cacheFile := caches()[0]
	before, _ := os.Stat(cacheFile)

	second := load()
	after, _ := os.Stat(cacheFile)
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("second load rebuilt the cache instead of reusing it")
	}
	for _, ds := range []*Dataset{first, second} {
		if got := queryRows(t, ds, "select code where phone = 620001"); fmt.Sprint(got) != "[[6]]" {
			t.Fatalf("rows = %v", got)
		}
		if ds.FieldTypes["phone"] != "string" || ds.FieldTypes["code"] != "int" || ds.RowCount != 2 {
			t.Fatalf("types = %v rows = %d", ds.FieldTypes, ds.RowCount)
		}
	}

	if err := os.WriteFile(path, []byte("phone,code\n620001,6\nsanity,1\n628333,9\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(time.Minute)
	os.Chtimes(path, future, future)
	third := load()
	if third.RowCount != 3 {
		t.Fatalf("changed file row count = %d, want 3", third.RowCount)
	}
	if got := caches(); len(got) != 1 || got[0] == cacheFile {
		t.Fatalf("caches after change = %v, want one new cache", got)
	}
}

func TestNoCacheFlagSkipsCache(t *testing.T) {
	cacheDir := t.TempDir()
	t.Setenv("JTV_CACHE_DIR", cacheDir)
	oldMin := csvCacheMinSize
	csvCacheMinSize = 0
	t.Cleanup(func() { csvCacheMinSize = oldMin; csvCacheDisabled = false })

	path := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(path, []byte("id\n1\n2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := run([]string{"--no-cache", "-f", path, "--csv", "-q", "select count(*)"}, strings.NewReader(""), &stdout, &stderr); err != nil {
		t.Fatalf("run: %v\n%s", err, stderr.String())
	}
	if stdout.String() != "count(*)\n2\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if matches, _ := filepath.Glob(filepath.Join(cacheDir, "*")); len(matches) != 0 {
		t.Fatalf("cache dir = %v, want empty", matches)
	}
}

func TestDoubleQuotedValuesActLikeSQLiteStrings(t *testing.T) {
	data := []byte("phone|code\n620004|6\n620005|1\n628222|11\n")
	ds, err := NewDataset(data, "test.psv")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	checks := map[string]string{
		`select count(*) where "phone" like "62000%"`:                           "[[2]]",
		`select count(*) where regexp_like("phone", "^6282")`:                   "[[1]]",
		`select "code" as "Kode" where "phone" = "628222"`:                      "[[11]]",
		`select "code" as "Kode" where "code" > 1 order by "Kode" desc limit 1`: "[[11]]",
	}
	for query, want := range checks {
		if got := queryRows(t, ds, query); fmt.Sprint(got) != want {
			t.Errorf("%s = %v, want %s", query, got, want)
		}
	}
}

func TestResultColumnsHideInternalNames(t *testing.T) {
	ds, err := NewDataset([]byte("price\n10\n20\n"), "test.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	result, err := ds.Query(context.Background(), "select count(*), sum(int(price))")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(result.Columns, "|"); got != "count(*)|sum(int(price))" {
		t.Fatalf("columns = %q", got)
	}
}

func TestOrderGroupsByKey(t *testing.T) {
	cases := map[string]string{
		`select "s", count(*) from input group by "s"`:                        `select "s", count(*) from input group by "s" order by "s"`,
		`select "s", count(*) from input group by "s" limit 5`:                `select "s", count(*) from input group by "s" order by "s" limit 5`,
		`select "s", count(*) as n from input group by "s" having n > 1`:      `select "s", count(*) as n from input group by "s" having n > 1 order by "s"`,
		`select "s", count(*) from input group by "s" order by count(*) desc`: `select "s", count(*) from input group by "s" order by count(*) desc`,
		`select "s" from input group by all order by min(jtv_rowid)`:          `select "s" from input group by all order by min(jtv_rowid)`,
		`select * from (select "s" from input group by "s") as t`:             `select * from (select "s" from input group by "s") as t`,
		`select "a", "b", sum(x) from input group by "a", "b";`:               `select "a", "b", sum(x) from input group by "a", "b" order by "a", "b";`,
	}
	for in, want := range cases {
		if got := orderGroupsByKey(in); got != want {
			t.Errorf("orderGroupsByKey(%q)\n got %q\nwant %q", in, got, want)
		}
	}
}

func TestMixedTextColumnSortsNumbersByValue(t *testing.T) {
	data := []byte("phone|name\n620009|a\nsanity|b\n62000|c\n-5|d\n6200099|e\n")
	ds, err := NewDataset(data, "test.psv")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	if got := queryRows(t, ds, "select name order by phone"); fmt.Sprint(got) != "[[d] [c] [a] [e] [b]]" {
		t.Fatalf("asc = %v", got)
	}
	if got := queryRows(t, ds, `select name order by "phone" desc limit 2`); fmt.Sprint(got) != "[[b] [e]]" {
		t.Fatalf("desc = %v", got)
	}
}

func TestOrderFilteredRows(t *testing.T) {
	cases := map[string]string{
		`select "a" from input where "b" = 1`:                  `select "a" from input where "b" = 1 order by jtv_rowid`,
		`select "a" from input where "b" = 1 limit 5`:          `select "a" from input where "b" = 1 order by jtv_rowid limit 5`,
		`select "a" from input where "b" = 1;`:                 `select "a" from input where "b" = 1 order by jtv_rowid;`,
		`select "a" from input where "b" = 1 order by "a"`:     "",
		`select "a" from input`:                                "",
		`select "a", count(*) from input where x group by "a"`: "",
		`select * from (select 1) t where 1 = 1`:               "",
	}
	for in, want := range cases {
		got, ok := orderFilteredRows(in)
		if !ok {
			got = ""
		}
		if got != want {
			t.Errorf("orderFilteredRows(%q)\n got %q\nwant %q", in, got, want)
		}
	}
}

func TestFilteredAggregateStillWorks(t *testing.T) {
	ds, err := NewDataset([]byte("a,b\n1,x\n2,y\n3,x\n"), "test.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	if got := queryRows(t, ds, "select count(*), sum(a) where b = 'x'"); fmt.Sprint(got) != "[[2 4]]" {
		t.Fatalf("aggregate = %v", got)
	}
	if got := queryRows(t, ds, "select a, b where b = 'x'"); fmt.Sprint(got) != "[[1 x] [3 x]]" {
		t.Fatalf("rows = %v", got)
	}
}

func TestMain(m *testing.M) {
	// A detached builder would re-run the test binary; build caches inline.
	backgroundCacheBuild = false
	os.Exit(m.Run())
}

func TestCacheBuilderProcessEntryPoint(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "data.csv")
	if err := os.WriteFile(source, []byte("phone|code\n620001|6\nsanity|1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cacheFile := filepath.Join(dir, "cache", "abc-def.duckdb")
	if err := os.MkdirAll(filepath.Dir(cacheFile), 0o755); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(dir, "cache", "abc-old.duckdb")
	os.WriteFile(stale, []byte("old"), 0o644)
	os.WriteFile(cacheFile+".lock", nil, 0o644)

	t.Setenv(cacheBuilderEnv, cacheFile)
	t.Setenv(cacheBuilderSourceEnv, source)
	t.Setenv(cacheBuilderCommaEnv, "|")
	t.Setenv(cacheBuilderHeaderEnv, "phone\x1fcode")
	if !isCacheBuilderProcess() {
		t.Fatal("builder env not recognized")
	}
	if code := runCacheBuilder(); code != 0 {
		t.Fatalf("runCacheBuilder = %d", code)
	}
	if _, err := os.Stat(cacheFile + ".lock"); !os.IsNotExist(err) {
		t.Fatal("lock file left behind")
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("stale cache of the same source not removed")
	}
	// The builder goes on to build key tables right after the cache.
	if _, err := os.Stat(keyFilePath(cacheFile, 0)); err != nil {
		t.Fatalf("key file not built after the cache: %v", err)
	}
	if locks, _ := filepath.Glob(filepath.Join(filepath.Dir(cacheFile), "*.lock")); len(locks) != 0 {
		t.Fatalf("locks left behind: %v", locks)
	}
	ds, err := openCachedCSV(cacheFile, []string{"phone", "code"}, source)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	if got := queryRows(t, ds, "select code where phone = 620001"); fmt.Sprint(got) != "[[6]]" {
		t.Fatalf("rows = %v", got)
	}
}

func TestStreamSessionReusesTableAcrossSchemas(t *testing.T) {
	session, err := newStreamSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	lines := []struct{ line, query, want string }{
		{`{"a":1,"b":"x"}`, "select a, b", "[[1 x]]"},
		{`{"a":2,"b":"y"}`, "select a, b", "[[2 y]]"},
		{`{"a":"text","c":true}`, "select a, c", "[[text 1]]"},
		{`{"a":3,"b":"z"}`, "select a, b", "[[3 z]]"},
	}
	for _, step := range lines {
		ds, err := newDataset([]byte(step.line), "", "stream", "", session)
		if err != nil {
			t.Fatal(err)
		}
		if got := queryRows(t, ds, step.query); fmt.Sprint(got) != step.want {
			t.Fatalf("%s: %s = %v, want %s", step.line, step.query, got, step.want)
		}
		if got := queryRows(t, ds, "select count(*)"); fmt.Sprint(got) != "[[1]]" {
			t.Fatalf("%s: rows left from earlier lines: %v", step.line, got)
		}
		ds.Close()
	}
}

func TestLateDecimalIsNotRoundedIntoIntegerColumn(t *testing.T) {
	// DuckDB's sniffer samples the first rows; a decimal far below them
	// must neither be rounded nor turn the column into text.
	var b strings.Builder
	b.WriteString("id|amount|big\n")
	for i := 1; i <= 30000; i++ {
		fmt.Fprintf(&b, "%d|%d|1\n", i, i)
	}
	b.WriteString("30001|4.5|1\n")
	b.WriteString("30002|7|1\n")
	ds, err := NewDataset([]byte(b.String()), "late.psv")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	if ds.FieldTypes["amount"] != "number" || ds.FieldTypes["id"] != "int" {
		t.Fatalf("types = %v", ds.FieldTypes)
	}
	if got := queryRows(t, ds, "select amount where id = 30001"); fmt.Sprint(got) != "[[4.5]]" {
		t.Fatalf("late decimal = %v", got)
	}
}

func TestIntegersBeyondDoublePrecisionStayExact(t *testing.T) {
	data := []byte("id|name\n1234567890123456789|a\n1234567890123456788|b\n")
	ds, err := NewDataset(data, "ids.psv")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	if got := queryRows(t, ds, "select id where name = 'b'"); fmt.Sprint(got) != "[[1234567890123456788]]" {
		t.Fatalf("id = %v", got)
	}
}

func TestAddKeyLookup(t *testing.T) {
	keys := map[string]string{"phone": "jtv_key_0"}
	cases := map[string]string{
		`select * from input where "phone" = '628'`:                          `select * from input where jtv_rowid in (select r from jtv_key_0 where k = '628')`,
		`select * from input where "code" = 1 and "phone" = '628' limit 5`:   `select * from input where "code" = 1 and jtv_rowid in (select r from jtv_key_0 where k = '628') limit 5`,
		`select * from input where "phone" in ('1', '2') order by jtv_rowid`: `select * from input where jtv_rowid in (select r from jtv_key_0 where k in ('1', '2')) order by jtv_rowid`,
		`select count(*) from input where "phone" ilike '62000%'`:            `select count(*) from input where jtv_rowid in (select r from jtv_key_0 where k like '62000%')`,
		`select count(*) from input where "phone" ilike 'abc%'`:              `select count(*) from input where "phone" ilike 'abc%'`,
		`select count(*) from input where "phone" = '1' or "code" = 2`:       `select count(*) from input where "phone" = '1' or "code" = 2`,
		`select count(*) from input where "phone" like '%62'`:                `select count(*) from input where "phone" like '%62'`,
		`select count(*) from input where "code" = 1`:                        `select count(*) from input where "code" = 1`,
		`select count(*) from input where not "phone" = '1'`:                 `select count(*) from input where not "phone" = '1'`,
	}
	for in, want := range cases {
		if got, missing := addKeyLookup(in, keys); got != want || missing != "" {
			t.Errorf("addKeyLookup(%q)\n got %q (missing %q)\nwant %q", in, got, missing, want)
		}
	}

	// A qualifying column without a built key table is reported, the query
	// left as is.
	query := `select * from input where "phone" = '628'`
	if got, missing := addKeyLookup(query, map[string]string{"phone": ""}); got != query || missing != "phone" {
		t.Errorf("unbuilt key: got %q, missing %q", got, missing)
	}
}

func TestCachedDatasetUsesKeyTables(t *testing.T) {
	t.Setenv("JTV_CACHE_DIR", t.TempDir())
	oldMin := csvCacheMinSize
	csvCacheMinSize = 0
	t.Cleanup(func() { csvCacheMinSize = oldMin })

	var b strings.Builder
	b.WriteString("phone|code\n")
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&b, "62000%06d|%d\n", i, i%7)
	}
	b.WriteString("SANITY1|3\n")
	path := filepath.Join(t.TempDir(), "phones.psv")
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	ds, err := NewDatasetFromInput(data, path, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	if _, ok := ds.keyCandidates["phone"]; !ok || len(ds.keyCandidates) != 1 {
		t.Fatalf("key candidates = %v, want phone only", ds.keyCandidates)
	}
	if ds.keyTables["phone"] != "" {
		t.Fatalf("key table built before any lookup: %v", ds.keyTables)
	}
	// The first lookup builds the key file (inline in tests); it lands next
	// to the cache and later lookups use it.
	if got := queryRows(t, ds, "select code where phone = 62000000009"); fmt.Sprint(got) != "[[2]]" {
		t.Fatalf("first lookup = %v", got)
	}
	if ds.keyTables["phone"] == "" {
		t.Fatal("key table not built by the first lookup")
	}
	if _, err := os.Stat(keyFilePath(ds.cacheFile, 0)); err != nil {
		t.Fatalf("key file: %v", err)
	}
	if rewritten := ds.rewrite("select * from input where \"phone\" = '1'"); !strings.Contains(rewritten, "jtv_key_0.keys") {
		t.Fatalf("lookup does not use the key table: %s", rewritten)
	}
	reopened, err := NewDatasetFromInput(data, path, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.keyTables["phone"] == "" {
		t.Fatal("reopened cache does not attach the existing key file")
	}
	checks := map[string]string{
		"select code where phone = 62000004321":                  "[[2]]",
		"select count(*) where phone like '6200000432%'":         "[[10]]",
		"select phone where phone in (62000000002, 62000000001)": "[[62000000001] [62000000002]]",
		"select code where phone = 'SANITY1'":                    "[[3]]",
		"select count(*) where phone = 62000000001 or code = 1":  "[[715]]",
	}
	for query, want := range checks {
		if got := queryRows(t, ds, query); fmt.Sprint(got) != want {
			t.Errorf("%s = %v, want %s", query, got, want)
		}
	}
}

func TestStreamedOutputMatchesCollectedOutput(t *testing.T) {
	ds, err := NewDataset([]byte(`[
		{"id":1,"name":"Ana <a&b>","score":1.5,"tags":["x"]},
		{"id":2,"name":"Budi","score":null,"tags":[]},
		{"id":3,"name":"Ci|ci","score":2,"tags":["y","z"]}
	]`), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	queries := []string{
		"select id, name, score",
		"select id, name, score where id > 10",
		"select count(*), sum(score)",
		"select id, tags",
	}
	writers := map[string]struct {
		opts    options
		collect func(io.Writer, *QueryResult) error
	}{
		"csv":   {options{csv: true}, writeCSV},
		"json":  {options{json: true}, writeJSON},
		"md":    {options{md: true}, writeMarkdown},
		"table": {options{}, func(w io.Writer, r *QueryResult) error { printTable(w, r); return nil }},
	}
	for _, query := range queries {
		result, err := ds.Query(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		for name, writer := range writers {
			var want, got bytes.Buffer
			if err := writer.collect(&want, result); err != nil {
				t.Fatal(err)
			}
			opts := writer.opts
			opts.query = query
			if err := streamQueryResult(ds, opts, &got); err != nil {
				t.Fatal(err)
			}
			if got.String() != want.String() {
				t.Errorf("%s %q:\n got %q\nwant %q", name, query, got.String(), want.String())
			}
		}
	}
}

func TestStreamedTableBeyondThreshold(t *testing.T) {
	var b strings.Builder
	b.WriteString("id,name\n")
	rows := tableStreamThreshold + 50
	for i := 1; i <= rows; i++ {
		fmt.Fprintf(&b, "%d,n%d\n", i, i)
	}
	ds, err := NewDataset([]byte(b.String()), "big.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	var out bytes.Buffer
	if err := streamQueryResult(ds, options{query: "select id, name"}, &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if last := lines[len(lines)-1]; last != fmt.Sprintf("%d row(s)", rows) {
		t.Fatalf("last line = %q", last)
	}
	// 3 header lines, the rows, a border and the count.
	if len(lines) != rows+5 {
		t.Fatalf("lines = %d, want %d", len(lines), rows+5)
	}
	if !strings.Contains(lines[len(lines)-3], fmt.Sprintf("n%d", rows)) {
		t.Fatalf("last row = %q", lines[len(lines)-3])
	}
}

func TestFailedQueryWritesNoOutputFile(t *testing.T) {
	ds, err := NewDataset([]byte(`[{"id":1}]`), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	path := filepath.Join(t.TempDir(), "out.csv")
	if err := streamQueryResult(ds, options{query: "select nope", output: path}, io.Discard); err == nil {
		t.Fatal("expected error")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("output file exists after failed query: %v", err)
	}
	empty := filepath.Join(t.TempDir(), "empty.csv")
	if err := streamQueryResult(ds, options{query: "select id where id > 5", output: empty}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(empty); string(data) != "id\n" {
		t.Fatalf("empty result file = %q", data)
	}
}

func TestChunkedQueriesMatchWholeQueries(t *testing.T) {
	var b strings.Builder
	b.WriteString("phone|code|note\n")
	for i := 0; i < 2500; i++ {
		fmt.Fprintf(&b, "62000%06d|%d|n%d\n", i, i%7, i%3)
	}
	b.WriteString("SANITY1|3|x\n")
	ds, err := NewDataset([]byte(b.String()), "chunks.psv")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	queries := []string{
		"select phone, code",
		"select * where code = 3",
		`select phone where note = 'n1' and code > 4`,
		"select count(*), sum(code) where code = 3",
		"select code, count(*) group by code",
		"select phone order by phone desc limit 5",
		"select phone limit 7",
		"select distinct code",
		"select phone, raw where code = 6",
	}
	old := chunkRows
	t.Cleanup(func() { chunkRows = old })
	for _, query := range queries {
		chunkRows = 1 << 30
		whole, err := ds.Query(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		chunkRows = 300
		chunked, err := ds.Query(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(whole.Columns, whole.Rows) != fmt.Sprint(chunked.Columns, chunked.Rows) {
			t.Errorf("%s: chunked result differs (%d vs %d rows)", query, len(chunked.Rows), len(whole.Rows))
		}
	}

	chunkRows = 300
	for _, query := range []string{"select *", "select * where code = 3"} {
		if _, ok := ds.chunkPlan(context.Background(), ds.rewrite(ds.applyQueryDefaults(query))); !ok {
			t.Errorf("%s should run in chunks", query)
		}
	}
	for _, query := range []string{"select count(*)", "select phone limit 3", "select code, count(*) group by code",
		"select distinct code", "select phone, code", "select * order by phone"} {
		if _, ok := ds.chunkPlan(context.Background(), ds.rewrite(ds.applyQueryDefaults(query))); ok {
			t.Errorf("%s should not run in chunks", query)
		}
	}
}

func TestKeyLookupsAreNotSplitIntoChunks(t *testing.T) {
	t.Setenv("JTV_CACHE_DIR", t.TempDir())
	oldMin, oldChunk := csvCacheMinSize, chunkRows
	csvCacheMinSize, chunkRows = 0, 1000
	t.Cleanup(func() { csvCacheMinSize, chunkRows = oldMin, oldChunk })

	var b strings.Builder
	b.WriteString("phone|code\n")
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&b, "62000%06d|%d\n", i, i%7)
	}
	path := filepath.Join(t.TempDir(), "phones.psv")
	os.WriteFile(path, []byte(b.String()), 0o644)
	data, _ := os.ReadFile(path)
	ds, err := NewDatasetFromInput(data, path, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	ds.rewrite(`select * from input where "phone" = '1'`) // builds the key table

	small := ds.rewrite(ds.applyQueryDefaults("select * where phone = 62000000042"))
	if _, ok := ds.chunkPlan(context.Background(), small); ok {
		t.Errorf("single key lookup should run whole: %s", small)
	}
	large := ds.rewrite(ds.applyQueryDefaults("select * where phone like '62000%'"))
	if _, ok := ds.chunkPlan(context.Background(), large); !ok {
		t.Errorf("key lookup matching all rows should run in chunks: %s", large)
	}
	for _, query := range []string{"select * where phone = 62000000042", "select * where phone like '620000001%'", "select * where phone like '62000%'"} {
		chunkRows = 1 << 30
		whole, err := ds.Query(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		chunkRows = 1000
		chunked, err := ds.Query(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(whole.Rows) != fmt.Sprint(chunked.Rows) {
			t.Errorf("%s: %d rows chunked vs %d whole", query, len(chunked.Rows), len(whole.Rows))
		}
	}
}

// startTestServer serves path from this process on its socket, like jtv
// serve does, until the test ends.
func startTestServer(t *testing.T, path string) {
	t.Helper()
	absPath, size, modTime, err := fileIdentity(path)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(absPath)
	ds, err := NewDatasetFromInput(data, absPath, absPath, "")
	if err != nil {
		t.Fatal(err)
	}
	socket := serveSocketPath(absPath)
	os.MkdirAll(filepath.Dir(socket), 0o755)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close(); ds.Close(); os.Remove(socket) })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			if serveConnection(conn, ds, absPath, size, modTime, "") {
				listener.Close()
				return
			}
		}
	}()
}

func TestServerAnswersLikeLocalRun(t *testing.T) {
	t.Setenv("JTV_CACHE_DIR", t.TempDir())
	dir := t.TempDir()
	path := filepath.Join(dir, "data.csv")
	os.WriteFile(path, []byte("id,name,score\n1,Ana,1.5\n2,Budi,\n3,Cici,7\n"), 0o644)

	run1 := func(args ...string) (string, error) {
		var stdout, stderr bytes.Buffer
		err := run(args, strings.NewReader(""), &stdout, &stderr)
		return stdout.String(), err
	}
	cases := [][]string{
		{"-f", path, "-q", "select id, name, score"},
		{"-f", path, "-q", "select id, name where score > 1", "--csv"},
		{"-f", path, "-q", "select * where id = 2", "--json"},
		{"-f", path, "-q", "count"},
		{"-f", path, "-q", "schema"},
		{"-f", path, "-q", "sum score", "--md"},
	}
	var local []string
	for _, args := range cases {
		out, err := run1(args...)
		if err != nil {
			t.Fatal(err)
		}
		local = append(local, out)
	}
	_, localErr := run1("-f", path, "-q", "select nope")

	startTestServer(t, path)
	for i, args := range cases {
		absPath, _, _, _ := fileIdentity(path)
		opts := options{file: path, query: args[3]}
		if len(args) > 4 {
			opts.csv, opts.json, opts.md = args[4] == "--csv", args[4] == "--json", args[4] == "--md"
		}
		var served bytes.Buffer
		handled, err := tryServer(opts, &served)
		if !handled || err != nil {
			t.Fatalf("%v: server not used (handled=%v err=%v, socket %s)", args, handled, err, serveSocketPath(absPath))
		}
		if served.String() != local[i] {
			t.Errorf("%v:\nserver %q\nlocal  %q", args, served.String(), local[i])
		}
	}
	if _, err := run1("-f", path, "-q", "select nope"); err == nil || err.Error() != localErr.Error() {
		t.Errorf("server error %v, local error %v", err, localErr)
	}

	out := filepath.Join(dir, "out.csv")
	if _, err := run1("-f", path, "-q", "select id where id > 1", "-o", out); err != nil {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(out); string(data) != "id\n2\n3\n" {
		t.Fatalf("-o through server = %q", data)
	}

	// A changed file stops the server; the client answers locally.
	os.WriteFile(path, []byte("id,name,score\n9,Zed,1\n"), 0o644)
	future := time.Now().Add(time.Minute)
	os.Chtimes(path, future, future)
	if got, err := run1("-f", path, "-q", "select name", "--csv"); err != nil || got != "name\nZed\n" {
		t.Fatalf("after change = %q, %v", got, err)
	}
}

func TestLooksHeaderless(t *testing.T) {
	cases := []struct {
		name string
		rows string
		want bool
	}{
		{"numbers only", "620000000013|6\n620000000014|20\n620000000099|16", true},
		{"name above numbers", "id|code\n1|6\n2|20", false},
		{"text columns only", "name|city\nAna|Jakarta\nBudi|Bandung", false},
		{"numbers and text, first row data", "1|Ana\n2|Budi\n3|Cici", true},
		{"header with numeric-looking name", "2024|total\n1|5\n2|6", false},
		{"single row", "1|2", false},
		{"booleans", "true|1\nfalse|2", true},
		{"name above booleans", "active|n\ntrue|1\nfalse|2", false},
	}
	for _, c := range cases {
		var sample [][]string
		for _, line := range strings.Split(c.rows, "\n") {
			sample = append(sample, strings.Split(line, "|"))
		}
		if got := looksHeaderless(sample); got != c.want {
			t.Errorf("%s: looksHeaderless = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestHeaderlessCSVGetsGeneratedNames(t *testing.T) {
	data := []byte("620000000013|6\n620000000014|20\n620000000099|16\n")
	ds, err := NewDataset(data, "crm.psv")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	if !ds.headerless || ds.RowCount != 3 {
		t.Fatalf("headerless=%v rows=%d, want true and 3", ds.headerless, ds.RowCount)
	}
	if got := queryRows(t, ds, "select col1, col2 where col2 = 6"); fmt.Sprint(got) != "[[620000000013 6]]" {
		t.Fatalf("rows = %v", got)
	}

	withHeader, err := NewDataset([]byte("phone|code\n620000000013|6\n"), "h.psv")
	if err != nil {
		t.Fatal(err)
	}
	defer withHeader.Close()
	if withHeader.headerless || withHeader.RowCount != 1 {
		t.Fatalf("header file: headerless=%v rows=%d", withHeader.headerless, withHeader.RowCount)
	}
}

func TestCSVHeaderFlag(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.csv")
	os.WriteFile(path, []byte("name,city\nAna,Jakarta\n"), 0o644)
	run1 := func(args ...string) string {
		var stdout, stderr bytes.Buffer
		if err := run(args, strings.NewReader(""), &stdout, &stderr); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		return stdout.String()
	}
	if got := run1("-f", path, "-q", "select count(*)", "--csv"); got != "count(*)\n1\n" {
		t.Fatalf("auto = %q", got)
	}
	if got := run1("-f", path, "-q", "select col1 order by col1", "--csv", "--csv-header", "no"); got != "col1\nAna\nname\n" {
		t.Fatalf("no header = %q", got)
	}
	if got := run1("-f", path, "-q", "select name", "--csv", "--csv-header", "yes"); got != "name\nAna\n" {
		t.Fatalf("yes = %q", got)
	}
	var stdout, stderr bytes.Buffer
	if err := run([]string{"-f", path, "-q", "select 1", "--csv-header", "maybe"}, strings.NewReader(""), &stdout, &stderr); err == nil {
		t.Fatal("invalid --csv-header accepted")
	}
}

func TestRenameColumnsIsSavedPerFile(t *testing.T) {
	t.Setenv("JTV_COLUMN_NAMES_DIR", t.TempDir())
	path := filepath.Join(t.TempDir(), "crm.psv")
	os.WriteFile(path, []byte("620000000013|6\n620000000014|20\nsanity|1\n"), 0o644)
	run1 := func(args ...string) (string, error) {
		var stdout, stderr bytes.Buffer
		err := run(args, strings.NewReader(""), &stdout, &stderr)
		return stdout.String(), err
	}
	if out, err := run1("-f", path, "-q", "rename col1 phone"); err != nil || !strings.Contains(out, "renamed col1 to phone (saved for this file)") {
		t.Fatalf("rename: %q %v", out, err)
	}
	if out, err := run1("-f", path, "-q", "rename col2 code"); err != nil {
		t.Fatalf("rename: %q %v", out, err)
	}
	if out, _ := run1("-f", path, "-q", "rename"); out != "col1 -> phone\ncol2 -> code\n" {
		t.Fatalf("list = %q", out)
	}
	if out, err := run1("-f", path, "-q", "select code where phone = 620000000014", "--csv"); err != nil || out != "code\n20\n" {
		t.Fatalf("query with saved names = %q %v", out, err)
	}
	if out, err := run1("-f", path, "-q", "select raw where code = 6", "--csv"); err != nil || !strings.Contains(out, `""phone""`) || strings.Contains(out, "col1") {
		t.Fatalf("raw = %q %v", out, err)
	}
	if out, _ := run1("-f", path, "-q", "schema"); !strings.Contains(out, "phone") || strings.Contains(out, "col1") {
		t.Fatalf("schema = %q", out)
	}
	// Renaming a renamed field again, and invalid renames.
	if _, err := run1("-f", path, "-q", "rename phone msisdn"); err != nil {
		t.Fatal(err)
	}
	if out, _ := run1("-f", path, "-q", "rename"); out != "col1 -> msisdn\ncol2 -> code\n" {
		t.Fatalf("list after second rename = %q", out)
	}
	for _, bad := range []string{"rename code msisdn", "rename nope x", "rename code raw", "rename a b c"} {
		if _, err := run1("-f", path, "-q", bad); err == nil {
			t.Errorf("%s: expected error", bad)
		}
	}
	if _, err := run1("-f", path, "-q", "rename reset"); err != nil {
		t.Fatal(err)
	}
	if out, err := run1("-f", path, "-q", "select col2 where col1 = 620000000013", "--csv"); err != nil || out != "col2\n6\n" {
		t.Fatalf("after reset = %q %v", out, err)
	}
}

func TestRenameKeepsKeyLookupsOnCachedData(t *testing.T) {
	t.Setenv("JTV_CACHE_DIR", t.TempDir())
	t.Setenv("JTV_COLUMN_NAMES_DIR", t.TempDir())
	oldMin := csvCacheMinSize
	csvCacheMinSize = 0
	t.Cleanup(func() { csvCacheMinSize = oldMin })

	var b strings.Builder
	for i := 0; i < 3000; i++ {
		fmt.Fprintf(&b, "62000%06d|%d\n", i, i%7)
	}
	path := filepath.Join(t.TempDir(), "phones.psv")
	os.WriteFile(path, []byte(b.String()), 0o644)
	data, _ := os.ReadFile(path)
	ds, err := NewDatasetFromInput(data, path, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	ds.useSavedColumnNames(path)
	var out bytes.Buffer
	if err := ds.renameCommand("col1 phone", &out); err != nil {
		t.Fatal(err)
	}
	queryRows(t, ds, "select col2 where phone = 62000000042") // builds the key table
	if got := ds.rewrite(`select * from input where "phone" = '1'`); !strings.Contains(got, "jtv_key_0.keys") {
		t.Fatalf("lookup on renamed column does not use the key table: %s", got)
	}
	if got := queryRows(t, ds, "select col2 where phone = 62000000042"); fmt.Sprint(got) != "[[0]]" {
		t.Fatalf("rows = %v", got)
	}
	if got := queryRows(t, ds, "select count(*) where phone like '6200000004%'"); fmt.Sprint(got) != "[[10]]" {
		t.Fatalf("prefix = %v", got)
	}
}

func TestWelcomeSuggestsNamingHeaderlessColumns(t *testing.T) {
	ds, err := NewDataset([]byte("620000000013|6\n620000000014|20\n"), "crm.psv")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	var out bytes.Buffer
	printWelcome(&out, ds)
	text := out.String()
	if !strings.Contains(text, "columns are named col1, col2") || !strings.Contains(text, "rename col1 NEW_NAME") {
		t.Fatalf("welcome = %q", text)
	}
}

func TestServerUsesRenamedColumns(t *testing.T) {
	t.Setenv("JTV_CACHE_DIR", t.TempDir())
	t.Setenv("JTV_COLUMN_NAMES_DIR", t.TempDir())
	path := filepath.Join(t.TempDir(), "crm.psv")
	os.WriteFile(path, []byte("620000000013|6\n620000000014|20\n"), 0o644)
	startTestServer(t, path)
	var stdout, stderr bytes.Buffer
	if err := run([]string{"-f", path, "-q", "rename col2 code"}, strings.NewReader(""), &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	if err := run([]string{"-f", path, "-q", "select code where col1 = 620000000014", "--csv"}, strings.NewReader(""), &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "code\n20\n" {
		t.Fatalf("server answer = %q", stdout.String())
	}
}

func TestFileTooBigForMemoryLoadsOnDisk(t *testing.T) {
	t.Setenv("JTV_NO_CACHE", "1")
	t.Setenv("JTV_MEM_AVAILABLE", "1000") // pretend memory is nearly full
	t.Setenv("TMPDIR", t.TempDir())
	var b strings.Builder
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&b, "%d|%d\n", 620000000000+i, i%7)
	}
	path := filepath.Join(t.TempDir(), "big.psv")
	os.WriteFile(path, []byte(b.String()), 0o644)
	data, _ := os.ReadFile(path)
	ds, err := NewDatasetFromInput(data, path, path, "")
	if err != nil {
		t.Fatal(err)
	}
	if ds.tempDB == "" {
		t.Fatal("expected an on-disk load database")
	}
	if _, err := os.Stat(ds.tempDB); err != nil {
		t.Fatalf("temp database: %v", err)
	}
	if got := queryRows(t, ds, "select count(*), sum(col2) where col1 >= 620000004990"); fmt.Sprint(got) != "[[10 28]]" {
		t.Fatalf("rows = %v", got)
	}
	tempDB := ds.tempDB
	ds.Close()
	if _, err := os.Stat(tempDB); !os.IsNotExist(err) {
		t.Fatalf("temp database left behind: %v", err)
	}
}

func TestFileTooBigForMemoryBuildsCacheDirectly(t *testing.T) {
	cacheDir := t.TempDir()
	t.Setenv("JTV_CACHE_DIR", cacheDir)
	t.Setenv("JTV_MEM_AVAILABLE", "1000")
	oldMin, oldBackground := csvCacheMinSize, backgroundCacheBuild
	// Background builds on: a file that does not fit in memory must still
	// be built here rather than handed to a background builder.
	csvCacheMinSize, backgroundCacheBuild = 0, true
	t.Cleanup(func() { csvCacheMinSize, backgroundCacheBuild = oldMin, oldBackground })

	var b strings.Builder
	for i := 0; i < 3000; i++ {
		fmt.Fprintf(&b, "%d|%d\n", i%5, i%7) // low cardinality: no key tables to spawn
	}
	path := filepath.Join(t.TempDir(), "big.psv")
	os.WriteFile(path, []byte(b.String()), 0o644)
	data, _ := os.ReadFile(path)
	ds, err := NewDatasetFromInput(data, path, path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	if ds.cacheFile == "" || ds.tempDB != "" {
		t.Fatalf("cacheFile=%q tempDB=%q, want the cache opened directly", ds.cacheFile, ds.tempDB)
	}
	if got := queryRows(t, ds, "select count(*)"); fmt.Sprint(got) != "[[3000]]" {
		t.Fatalf("count = %v", got)
	}
	if locks, _ := filepath.Glob(filepath.Join(cacheDir, "*.lock")); len(locks) != 0 {
		t.Fatalf("locks left: %v", locks)
	}
	if limit := buildMemoryLimit(); limit != "1024MB" {
		t.Fatalf("memory limit = %s, want the 1GB floor", limit)
	}
}

// referenceStream is --stream as it worked before batching: every line on
// its own. The batched implementation must match it byte for byte.
func referenceStream(t *testing.T, input string, opts options) (string, string) {
	t.Helper()
	session, err := newStreamSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	var stdout, stderr bytes.Buffer
	writer := newStreamWriter(&stdout, opts)
	for number, line := range strings.Split(input, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		ds, err := newDataset([]byte(line), "", fmt.Sprintf("stream:%d", number+1), opts.delimiter, session)
		if err != nil {
			fmt.Fprintf(&stderr, "stream line %d: %v\n", number+1, err)
			continue
		}
		result, err := ds.Query(context.Background(), opts.query)
		ds.Close()
		if err != nil {
			fmt.Fprintf(&stderr, "stream line %d: %v\n", number+1, err)
			continue
		}
		if err := writer.Write(result); err != nil {
			t.Fatal(err)
		}
	}
	writer.Close()
	return stdout.String(), stderr.String()
}

// captureStderr runs f with os.Stderr redirected and returns what it wrote.
func captureStderr(t *testing.T, f func()) string {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stderr
	os.Stderr = write
	done := make(chan string)
	go func() {
		data, _ := io.ReadAll(read)
		done <- string(data)
	}()
	f()
	os.Stderr = saved
	write.Close()
	return <-done
}

func TestBatchedStreamMatchesLineByLine(t *testing.T) {
	var b strings.Builder
	for i := 1; i <= 300; i++ {
		switch {
		case i%37 == 0:
			b.WriteString("not json\n")
		case i%41 == 0:
			b.WriteString("\n")
		case i%29 == 0:
			fmt.Fprintf(&b, `{"time":"t%d","tags":["a","b"],"status":200}`+"\n", i)
		case i%23 == 0:
			fmt.Fprintf(&b, `{"time":"t%d","status":"%d"}`+"\n", i, 500)
		case i%7 == 0:
			fmt.Fprintf(&b, `{"time":"t%d","status":500,"duration_s":%d.5,"note":"Rp %d.000"}`+"\n", i, i, i)
		default:
			fmt.Fprintf(&b, `{"time":"t%d","status":%d,"duration_s":%d}`+"\n", i, 200+(i%3)*100, i)
		}
	}
	input := b.String()

	queries := []string{
		"select time, status, duration_s",
		"select * where status = 500",
		"select count(*)",
		"select time where duration_s > 150",
		"select status limit 1",
		"select distinct status",
		"select time, int(note) as amount where note is not null",
		"select time, tags",
		"select nope",
		"select time where regexp_like(time, 't1[0-9]$')",
		"select time where status = 999",
		"select time where status = 999 limit 1",
		"select count(*) where status = 999",
	}
	formats := map[string]options{"csv": {csv: true}, "json": {json: true}, "table": {}, "md": {md: true}}
	for _, query := range queries {
		for name, format := range formats {
			opts := format
			opts.query = query
			wantOut, wantErr := referenceStream(t, input, opts)

			var gotOut bytes.Buffer
			gotErr := captureStderr(t, func() {
				session, err := newStreamSession()
				if err != nil {
					t.Fatal(err)
				}
				defer session.Close()
				writer := newStreamWriter(&gotOut, opts)
				if err := runStreamBatched(strings.NewReader(input), session, writer, opts, query); err != nil {
					t.Fatal(err)
				}
				writer.Close()
			})
			if gotOut.String() != wantOut {
				t.Errorf("%s %q: stdout differs\n got %d bytes: %.300q\nwant %d bytes: %.300q", name, query, gotOut.Len(), gotOut.String(), len(wantOut), wantOut)
			}
			if gotErr != wantErr {
				t.Errorf("%s %q: stderr differs\n got %.300q\nwant %.300q", name, query, gotErr, wantErr)
			}
		}
	}
}

func TestStreamFilterMatchingNothingIsStillBatched(t *testing.T) {
	session, err := newStreamSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	ds, err := newDataset([]byte(`{"time":"t1","status":200}`), "", "stream:1", "", session)
	if err != nil {
		t.Fatal(err)
	}
	runner := &streamRunner{session: session, query: "select time where status = 500"}
	if !runner.queryShapeIsBatchable(ds) {
		t.Fatal("a filter that matches nothing on the first line should still batch")
	}
	runner.query = "select count(*) where status = 500"
	if runner.queryShapeIsBatchable(ds) {
		t.Fatal("an aggregate must not batch")
	}
	runner.query = "select time where status = 500 limit 1"
	if runner.queryShapeIsBatchable(ds) {
		t.Fatal("a limit must not batch")
	}
}

func TestCopyCSVExportMatchesGoWriter(t *testing.T) {
	jsonData := []byte(`[
		{"s":"a,b","n":1,"f":1.5,"b":true,"plain":"x"},
		{"s":"say \"hi\"","n":-2,"f":2,"b":false,"plain":"y"},
		{"s":"line\nbreak","n":3,"f":0.1,"b":true,"plain":"z"},
		{"s":"","n":4,"f":1e21,"b":false,"plain":""},
		{"s":null,"n":5,"f":1e-7,"b":true,"plain":null},
		{"s":"trail ","n":7,"f":-0.5,"b":true,"plain":"w"},
		{"s":"tab\tin","n":9,"f":4.25,"b":true,"plain":"v"},
		{"s":"héllo ✓","n":10,"f":5,"b":false,"plain":"u"},
		{"s":"x;y","n":11,"f":6,"b":true,"plain":"t"},
		{"s":"cr\rhere","n":12,"f":7,"b":false,"plain":"s"}
	]`)
	jsonLead := []byte(`[{"s":" lead","n":1},{"s":"\\.","n":2},{"s":"ok","n":3}]`)
	csvData := []byte("620000000013|6\n620000000014|20\nsanity|1\n|11\n-620000000096|16\n")

	cases := []struct {
		name, data, query string
		fast              bool
	}{
		{"json", string(jsonData), "select s, n, b, plain", true},
		{"json", string(jsonData), "select s, n where n > 3", true},
		{"json", string(jsonData), "select n, count(*) as total group by n", true},
		{"json", string(jsonData), `select n, sum(n) as "sum(a, b)" group by n`, true},
		{"json", string(jsonData), "select s where n > 100", true},
		{"json", string(jsonData), "select s, f", false},
		{"json", string(jsonData), "select date('2024-01-02') as d, n", true}, // date() returns text
		{"json", string(jsonData), "select n / 2 as half, s", false},
		{"lead", string(jsonLead), "select s, n", false},
		{"csv", string(csvData), "select col1, col2", true},
		{"csv", string(csvData), "select * where col2 = 6 or col1 = 'sanity'", true},
		{"csv", string(csvData), "select col1", true},
	}
	t.Cleanup(func() { csvCopyDisabled = false })
	for _, c := range cases {
		ds, err := NewDataset([]byte(c.data), c.name)
		if err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		fastPath, slowPath := filepath.Join(dir, "fast.csv"), filepath.Join(dir, "slow.csv")

		csvCopyDisabled = false
		handled, err := tryCopyCSV(ds, c.query, fastPath)
		if err != nil {
			t.Fatalf("%s %q: %v", c.name, c.query, err)
		}
		if handled != c.fast {
			t.Errorf("%s %q: fast path used = %v, want %v", c.name, c.query, handled, c.fast)
		}
		csvCopyDisabled = false
		os.Remove(fastPath)
		if err := streamQueryResult(ds, options{query: c.query, output: fastPath}, io.Discard); err != nil {
			t.Fatal(err)
		}
		csvCopyDisabled = true
		if err := streamQueryResult(ds, options{query: c.query, output: slowPath}, io.Discard); err != nil {
			t.Fatal(err)
		}
		fast, _ := os.ReadFile(fastPath)
		slow, _ := os.ReadFile(slowPath)
		if string(fast) != string(slow) {
			t.Errorf("%s %q: exports differ\nfast %q\nslow %q", c.name, c.query, fast, slow)
		}
		ds.Close()
	}
}
