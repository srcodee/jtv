package main

import "github.com/srcodee/jtv/internal/jtvcore"

type Dataset = jtvcore.Dataset
type QueryResult = jtvcore.QueryResult

var NewDataset = jtvcore.NewDataset
var resultRowsAsObjects = jtvcore.ResultRowsAsObjects
var writeCSV = jtvcore.WriteCSV
var writeJSON = jtvcore.WriteJSON

func hasTopLevelLimit(query string) bool {
	return jtvcore.HasTopLevelLimit(query)
}
