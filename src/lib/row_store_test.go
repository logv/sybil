package sybil

import (
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

func TestTableLoadRowRecords(test *testing.T) {
	deleteTestDB()

	blockCount := 3
	addRecordsToTestDB(func(r *Record, index int) {
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age)
		r.AddStrField("ageStr", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	t := GetTable(testTableName)
	t.IngestRecords("ingest")

	unloadTestTable()
	nt := GetTable(testTableName)

	nt.LoadRecords(nil)

	if len(nt.RowBlock.RecordList) != CHUNK_SIZE*blockCount {
		test.Error("Row Store didn't read back right number of records", len(nt.RowBlock.RecordList))
	}

	if len(nt.BlockList) != 1 {
		test.Error("Found other records than rowblock")
	}

	querySpec := newTestQuerySpec()

	querySpec.Groups = append(querySpec.Groups, nt.Grouping("ageStr"))
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation("age", "avg"))

	nt.MatchAndAggregate(querySpec)

	// Test that the group by and int keys are correctly re-assembled
	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		val, err := strconv.ParseInt(k, 10, 64)
		if err != nil || math.Abs(float64(val)-float64(v.Hists["age"].Mean())) > 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, val, v.Hists["age"].Mean())
		}
	}

}
