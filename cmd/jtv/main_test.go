package main

import (
	"bytes"
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
