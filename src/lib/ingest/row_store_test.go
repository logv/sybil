package sybil

import (
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	. "github.com/logv/sybil/src/lib/aggregate"
	. "github.com/logv/sybil/src/lib/column_store"
	. "github.com/logv/sybil/src/lib/record"
	. "github.com/logv/sybil/src/lib/specs"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/lib/test_helpers"
)

func TestTableLoadRowRecords(test *testing.T) {
	DeleteTestDB()

	blockCount := 3
	AddRecordsToTestDB(func(r *Record, index int) {
		AddIntField(r, "id", int64(index))
		age := int64(rand.Intn(20)) + 10
		AddIntField(r, "age", age)
		AddStrField(r, "ageStr", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	t := GetTable(TEST_TABLE_NAME)
	IngestRecords(t, "ingest")

	UnloadTestTable()
	nt := GetTable(TEST_TABLE_NAME)

	LoadRecords(nt, nil)

	if len(nt.RowBlock.RecordList) != CHUNK_SIZE*blockCount {
		test.Error("Row Store didn't read back right number of records", len(nt.RowBlock.RecordList))
	}

	if len(nt.BlockList) != 1 {
		test.Error("Found other records than rowblock")
	}

	querySpec := NewTestQuerySpec()

	querySpec.Groups = append(querySpec.Groups, GroupingForTable(nt, "ageStr"))
	querySpec.Aggregations = append(querySpec.Aggregations, AggregationForTable(nt, "age", "avg"))

	MatchAndAggregate(nt, querySpec)

	// Test that the group by and int keys are correctly re-assembled
	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		val, err := strconv.ParseInt(k, 10, 64)
		if err != nil || math.Abs(float64(val)-float64(v.Hists["age"].Mean())) > 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, val, v.Hists["age"].Mean())
		}
	}

}
