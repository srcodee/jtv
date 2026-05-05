package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
)

const maxBarWidth = 32

func runChartCommand(ds *Dataset, out io.Writer, command, arg string) error {
	switch command {
	case "bar":
		return runBar(ds, out, arg)
	case "top":
		return runTop(ds, out, arg)
	case "hist":
		return runHist(ds, out, arg)
	case "line":
		return runLine(ds, out, arg)
	case "chart":
		kind, rest := splitCommand(arg)
		if kind == "" {
			return errors.New("usage: chart bar SELECT ...")
		}
		return runChartCommand(ds, out, kind, rest)
	default:
		return fmt.Errorf("unknown chart %q", command)
	}
}

func runBar(ds *Dataset, out io.Writer, arg string) error {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return errors.New("usage: bar FIELD or bar SELECT ...")
	}
	query := arg
	if !isSelectQuery(arg) {
		query = fmt.Sprintf("select %s, count(*) as total group by %s order by total desc", arg, arg)
	}
	result, err := ds.Query(context.Background(), query)
	if err != nil {
		return err
	}
	return printBarChart(out, result)
}

func runTop(ds *Dataset, out io.Writer, arg string) error {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return errors.New("usage: top FIELD [N]")
	}
	parts := strings.Fields(arg)
	field := parts[0]
	limit := 10
	if len(parts) > 1 {
		if n, err := strconv.Atoi(parts[1]); err == nil && n > 0 {
			limit = n
		}
	}
	query := fmt.Sprintf("select %s, count(*) as total group by %s order by total desc limit %d", field, field, limit)
	result, err := ds.Query(context.Background(), query)
	if err != nil {
		return err
	}
	return printBarChart(out, result)
}

func runHist(ds *Dataset, out io.Writer, arg string) error {
	field := strings.TrimSpace(arg)
	if field == "" {
		return errors.New("usage: hist FIELD")
	}
	result, err := ds.Query(context.Background(), fmt.Sprintf("select %s", field))
	if err != nil {
		return err
	}
	values := numericValues(result)
	if len(values) == 0 {
		return errors.New("hist needs numeric values")
	}
	printHistogram(out, field, values)
	return nil
}

func runLine(ds *Dataset, out io.Writer, arg string) error {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return errors.New("usage: line [spark|full|points] X_FIELD Y_FIELD or line [spark|full|points] SELECT ...")
	}

	variant := "spark"
	if first, rest := splitCommand(arg); first == "spark" || first == "full" || first == "points" {
		variant = first
		arg = rest
	}
	if arg == "" {
		return errors.New("usage: line [spark|full|points] X_FIELD Y_FIELD")
	}
	query := arg
	if !isSelectQuery(arg) {
		parts := strings.Fields(arg)
		if len(parts) < 2 {
			return errors.New("usage: line [spark|full|points] X_FIELD Y_FIELD")
		}
		query = fmt.Sprintf("select %s, %s", parts[0], parts[1])
	}
	result, err := ds.Query(context.Background(), query)
	if err != nil {
		return err
	}
	return printLineChart(out, result, variant)
}

func printBarChart(out io.Writer, result *QueryResult) error {
	if len(result.Columns) < 2 {
		return errors.New("bar chart needs two columns: label and value")
	}
	if len(result.Rows) == 0 {
		fmt.Fprintln(out, "no rows found")
		return nil
	}

	items := make([]chartItem, 0, len(result.Rows))
	maxValue := 0.0
	total := 0.0
	labelWidth := displayLen(result.Columns[0])
	for _, row := range result.Rows {
		if len(row) < 2 {
			continue
		}
		value, ok := parseFloat(row[1])
		if !ok {
			return fmt.Errorf("bar value %q is not numeric", row[1])
		}
		items = append(items, chartItem{label: row[0], value: value})
		maxValue = math.Max(maxValue, value)
		total += value
		labelWidth = max(labelWidth, displayLen(row[0]))
	}
	if maxValue == 0 {
		maxValue = 1
	}

	fmt.Fprintf(out, "%-*s | %s\n", labelWidth, result.Columns[0], result.Columns[1])
	fmt.Fprintf(out, "%s-+-%s\n", strings.Repeat("-", labelWidth), strings.Repeat("-", maxBarWidth+16))
	for _, item := range items {
		width := int(math.Round((item.value / maxValue) * maxBarWidth))
		if item.value > 0 && width == 0 {
			width = 1
		}
		percent := 0.0
		if total > 0 {
			percent = item.value / total * 100
		}
		fmt.Fprintf(
			out,
			"%-*s | %-*s %s %5.1f%%\n",
			labelWidth,
			item.label,
			maxBarWidth,
			strings.Repeat("#", width),
			formatNumber(item.value),
			percent,
		)
	}
	return nil
}

func printHistogram(out io.Writer, field string, values []float64) {
	sort.Float64s(values)
	minValue := values[0]
	maxValue := values[len(values)-1]
	if minValue == maxValue {
		fmt.Fprintf(out, "%s\n", field)
		fmt.Fprintf(out, "%s | %s %d\n", formatNumber(minValue), strings.Repeat("#", maxBarWidth), len(values))
		return
	}

	bins := 10
	if len(values) < bins {
		bins = len(values)
	}
	counts := make([]int, bins)
	step := (maxValue - minValue) / float64(bins)
	for _, value := range values {
		index := int((value - minValue) / step)
		if index >= bins {
			index = bins - 1
		}
		counts[index]++
	}
	maxCount := 1
	labelWidth := 0
	labels := make([]string, bins)
	for i := 0; i < bins; i++ {
		start := minValue + float64(i)*step
		end := start + step
		labels[i] = fmt.Sprintf("%s-%s", formatNumber(start), formatNumber(end))
		labelWidth = max(labelWidth, displayLen(labels[i]))
		maxCount = max(maxCount, counts[i])
	}

	fmt.Fprintln(out, field)
	for i, count := range counts {
		width := int(math.Round((float64(count) / float64(maxCount)) * maxBarWidth))
		if count > 0 && width == 0 {
			width = 1
		}
		fmt.Fprintf(out, "%-*s | %-*s %d\n", labelWidth, labels[i], maxBarWidth, strings.Repeat("#", width), count)
	}
}

func printLineChart(out io.Writer, result *QueryResult, variant string) error {
	if len(result.Columns) < 2 {
		return errors.New("line chart needs two columns: x and numeric y")
	}
	points, err := linePoints(result)
	if err != nil {
		return err
	}
	if len(points) == 0 {
		fmt.Fprintln(out, "no rows found")
		return nil
	}
	if len(points) == 1 {
		fmt.Fprintf(out, "%s -> %s\n", points[0].label, formatNumber(points[0].value))
		return nil
	}
	switch variant {
	case "spark":
		printSparkline(out, result.Columns[0], result.Columns[1], points)
		return nil
	case "full":
		return printFullLineChart(out, result.Columns[0], result.Columns[1], points)
	case "points":
		return printPointChart(out, result.Columns[0], result.Columns[1], points)
	default:
		return fmt.Errorf("unknown line variant %q; use spark, full, or points", variant)
	}
}

func linePoints(result *QueryResult) ([]linePoint, error) {
	points := make([]linePoint, 0, len(result.Rows))
	for _, row := range result.Rows {
		if len(row) < 2 {
			continue
		}
		value, ok := parseFloat(row[1])
		if !ok {
			return nil, fmt.Errorf("line value %q is not numeric", row[1])
		}
		points = append(points, linePoint{label: row[0], value: value})
	}
	return points, nil
}

func printSparkline(out io.Writer, xColumn, yColumn string, points []linePoint) {
	if len(points) > 80 {
		points = sampleLinePoints(points, 80)
	}
	minValue, maxValue := lineMinMax(points)
	blocks := []rune("▁▂▃▄▅▆▇█")
	var b strings.Builder
	for _, point := range points {
		index := 0
		if maxValue > minValue {
			index = int(math.Round(((point.value - minValue) / (maxValue - minValue)) * float64(len(blocks)-1)))
		}
		if index < 0 {
			index = 0
		}
		if index >= len(blocks) {
			index = len(blocks) - 1
		}
		b.WriteRune(blocks[index])
	}
	fmt.Fprintf(out, "%s by %s\n", yColumn, xColumn)
	fmt.Fprintf(out, "%s %s %s\n", points[0].label, b.String(), points[len(points)-1].label)
	fmt.Fprintf(out, "min %s | max %s | points %d\n", formatNumber(minValue), formatNumber(maxValue), len(points))
}

func printFullLineChart(out io.Writer, xColumn, yColumn string, points []linePoint) error {
	if len(points) > 60 {
		points = sampleLinePoints(points, 60)
	}

	height := 10
	width := len(points)
	minValue, maxValue := lineMinMax(points)
	if minValue == maxValue {
		maxValue = minValue + 1
	}
	grid := make([][]rune, height)
	for i := range grid {
		grid[i] = []rune(strings.Repeat(" ", width))
	}
	for x, point := range points {
		y := lineY(point.value, minValue, maxValue, height)
		marker := '*'
		if x > 0 {
			prevY := lineY(points[x-1].value, minValue, maxValue, height)
			switch {
			case y < prevY:
				marker = '/'
			case y > prevY:
				marker = '\\'
			default:
				marker = '-'
			}
		}
		grid[y][x] = marker
	}

	fmt.Fprintf(out, "%s by %s\n", yColumn, xColumn)
	for y, row := range grid {
		value := maxValue - (float64(y)/float64(height-1))*(maxValue-minValue)
		fmt.Fprintf(out, "%8s | %s\n", formatNumber(value), string(row))
	}
	fmt.Fprintf(out, "%8s + %s\n", "", strings.Repeat("-", width))
	fmt.Fprintf(out, "%8s   %s", "", points[0].label)
	if width > displayLen(points[0].label)+displayLen(points[len(points)-1].label)+2 {
		fmt.Fprintf(out, "%*s", width-displayLen(points[0].label), points[len(points)-1].label)
	}
	fmt.Fprintln(out)
	return nil
}

func lineY(value, minValue, maxValue float64, height int) int {
	ratio := (value - minValue) / (maxValue - minValue)
	y := height - 1 - int(math.Round(ratio*float64(height-1)))
	if y < 0 {
		return 0
	}
	if y >= height {
		return height - 1
	}
	return y
}

func printPointChart(out io.Writer, xColumn, yColumn string, points []linePoint) error {
	if len(points) > 60 {
		points = sampleLinePoints(points, 60)
	}
	height := 10
	width := len(points)
	minValue, maxValue := lineMinMax(points)
	if minValue == maxValue {
		maxValue = minValue + 1
	}
	grid := make([][]rune, height)
	for i := range grid {
		grid[i] = []rune(strings.Repeat(" ", width))
	}
	for x, point := range points {
		ratio := (point.value - minValue) / (maxValue - minValue)
		y := height - 1 - int(math.Round(ratio*float64(height-1)))
		if y < 0 {
			y = 0
		}
		if y >= height {
			y = height - 1
		}
		grid[y][x] = '*'
	}
	fmt.Fprintf(out, "%s by %s\n", yColumn, xColumn)
	for y, row := range grid {
		value := maxValue - (float64(y)/float64(height-1))*(maxValue-minValue)
		fmt.Fprintf(out, "%8s | %s\n", formatNumber(value), string(row))
	}
	fmt.Fprintf(out, "%8s + %s\n", "", strings.Repeat("-", width))
	fmt.Fprintf(out, "%8s   %s", "", points[0].label)
	if width > displayLen(points[0].label)+displayLen(points[len(points)-1].label)+2 {
		fmt.Fprintf(out, "%*s", width-displayLen(points[0].label), points[len(points)-1].label)
	}
	fmt.Fprintln(out)
	return nil
}

func sampleLinePoints(points []linePoint, limit int) []linePoint {
	if len(points) <= limit {
		return points
	}
	out := make([]linePoint, 0, limit)
	step := float64(len(points)-1) / float64(limit-1)
	for i := 0; i < limit; i++ {
		index := int(math.Round(float64(i) * step))
		if index >= len(points) {
			index = len(points) - 1
		}
		out = append(out, points[index])
	}
	return out
}

func lineMinMax(points []linePoint) (float64, float64) {
	minValue := points[0].value
	maxValue := points[0].value
	for _, point := range points[1:] {
		minValue = math.Min(minValue, point.value)
		maxValue = math.Max(maxValue, point.value)
	}
	return minValue, maxValue
}

type chartItem struct {
	label string
	value float64
}

type linePoint struct {
	label string
	value float64
}

func numericValues(result *QueryResult) []float64 {
	values := make([]float64, 0, len(result.Rows))
	for _, row := range result.Rows {
		if len(row) == 0 {
			continue
		}
		if value, ok := parseFloat(row[0]); ok {
			values = append(values, value)
		}
	}
	return values
}

func parseFloat(value string) (float64, bool) {
	f, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	return f, err == nil
}

func formatNumber(value float64) string {
	if math.Trunc(value) == value {
		return strconv.FormatInt(int64(value), 10)
	}
	return strconv.FormatFloat(value, 'f', 2, 64)
}
