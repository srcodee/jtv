package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunBarField(t *testing.T) {
	ds := testChartDataset(t)
	defer ds.Close()
	var out bytes.Buffer

	err := runChartCommand(ds, &out, "bar", "status")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "ok") || !strings.Contains(out.String(), "fail") {
		t.Fatalf("missing chart labels: %s", out.String())
	}
}

func TestRunBarSelect(t *testing.T) {
	ds := testChartDataset(t)
	defer ds.Close()
	var out bytes.Buffer

	err := runChartCommand(ds, &out, "bar", "select status, count(*) as total group by status")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "total") {
		t.Fatalf("missing value column: %s", out.String())
	}
}

func TestRunHist(t *testing.T) {
	ds := testChartDataset(t)
	defer ds.Close()
	var out bytes.Buffer

	err := runChartCommand(ds, &out, "hist", "likes")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "likes") {
		t.Fatalf("missing hist title: %s", out.String())
	}
}

func TestRunLineFields(t *testing.T) {
	ds := testChartDataset(t)
	defer ds.Close()
	var out bytes.Buffer

	err := runChartCommand(ds, &out, "line", "likes likes")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "likes by likes") {
		t.Fatalf("missing line title: %s", out.String())
	}
	if !strings.Contains(out.String(), "min") || !strings.Contains(out.String(), "max") {
		t.Fatalf("default line should be sparkline: %s", out.String())
	}
}

func TestRunLineSelect(t *testing.T) {
	ds := testChartDataset(t)
	defer ds.Close()
	var out bytes.Buffer

	err := runChartCommand(ds, &out, "line", "select likes, likes order by likes")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "min") {
		t.Fatalf("missing sparkline metadata: %s", out.String())
	}
}

func TestRunLineVariants(t *testing.T) {
	ds := testChartDataset(t)
	defer ds.Close()

	var full bytes.Buffer
	if err := runChartCommand(ds, &full, "line", "full likes likes"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(full.String(), "+") {
		t.Fatalf("missing full chart axis: %s", full.String())
	}

	var points bytes.Buffer
	if err := runChartCommand(ds, &points, "line", "points likes likes"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(points.String(), "*") {
		t.Fatalf("missing point chart markers: %s", points.String())
	}
}

func testChartDataset(t *testing.T) *Dataset {
	t.Helper()
	ds, err := NewDataset([]byte(`[
		{"status":"ok","likes":1},
		{"status":"ok","likes":2},
		{"status":"fail","likes":8}
	]`), "test")
	if err != nil {
		t.Fatal(err)
	}
	return ds
}
