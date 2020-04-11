package main

import api "github.com/logv/sybil/src/api"

import "flag"

var TEST_DB = "testdb"

type FlagDefs struct {
	sybilBin string
}

var FLAGS FlagDefs

func setupFlags() {
	flag.StringVar(&FLAGS.sybilBin, "bin", "sybil", "location to sybil binary")
}

func init() {
	setupFlags()
}

func main() {
	config := api.SybilConfig{Dir: TEST_DB, Table: "test_structs"}

	records := genStructRecords(500)

	table := api.NewTable(&config)

	// Add records takes []interface{} which can be of multiple types:
	// * []SybilMapRecord
	// * [][]byte (raw JSON)
	// * []map[string]interface{}
	// * []interface{} (generic structs)
	table.AddRecords(records)
	table.FlushRecords()

	tables := api.ListTables(&config)
	api.Print("TABLES", tables)

	queried, err := table.
		Query().
		GroupBy("name").
		Aggregate("age").
		LogHist().
		Limit(1).
		Execute()

	if err == nil {
		api.Debug("QUERIED", queried)
	}
}
