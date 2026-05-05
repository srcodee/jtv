package jtvcore

import (
	"encoding/csv"
	"encoding/json"
	"io"
)

func ResultRowsAsObjects(result *QueryResult) []map[string]any {
	rows := make([]map[string]any, 0, len(result.Values))
	for _, values := range result.Values {
		row := make(map[string]any, len(result.Columns))
		for i, column := range result.Columns {
			if i < len(values) {
				row[column] = JSONValue(values[i])
			}
		}
		rows = append(rows, row)
	}
	return rows
}

func JSONValue(value any) any {
	switch v := value.(type) {
	case []byte:
		return string(v)
	default:
		return v
	}
}

func WriteCSV(w io.Writer, result *QueryResult) error {
	writer := csv.NewWriter(w)
	if err := writer.Write(result.Columns); err != nil {
		return err
	}
	for _, row := range result.Rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func WriteJSON(w io.Writer, result *QueryResult) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(ResultRowsAsObjects(result))
}
