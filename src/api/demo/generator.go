package main

import "strconv"
import "math/rand"
import api "github.com/logv/sybil/src/api"

var NAMES = []string{"john", "joe", "mary", "paul", "peter", "dave", "abe"}

type SampleRecord struct {
	Age  int      `json:"age"`
	Name string   `json:"name"`
	Abc  []string `json:"abc"`
}

// Example of using struct Sample Builder
func genStructRecords(n int) []interface{} {
	records := make([]interface{}, 0)
	for i := 0; i < n; i++ {
		r := SampleRecord{}
		r.Age = rand.Int()%70 + 10

		name := NAMES[rand.Int()%len(NAMES)]
		r.Name = name

		set := make([]string, 0)
		for j := 0; j < rand.Int()%10; j++ {
			set = append(set, strconv.Itoa(rand.Int()%50+23))
		}

		r.Abc = set
		records = append(records, r)
	}

	return records
}

// Example of using JSON sample builder
func genJSONRecords(n int) [][]byte {
	records := make([][]byte, 0)
	for i := 0; i < n; i++ {
		r := api.NewRecord()
		r.Int("age", rand.Int()%70+10)

		name := NAMES[rand.Int()%len(NAMES)]
		r.Str("name", name)

		set := make([]string, 0)
		for j := 0; j < rand.Int()%10; j++ {
			set = append(set, strconv.Itoa(rand.Int()%50+23))
		}

		r.Set("abc", set)
		records = append(records, r.JSON())
	}

	return records
}
