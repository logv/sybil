package sybil

import "strconv"
import "strings"
import "math"
import "math/rand"
import "testing"

func TestTableLoadRowRecords(t *testing.T) {
	t.Parallel()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)

	blockCount := 3
	addRecords(tableName, func(r *Record, index int) {
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	tbl := GetTable(tableName)
	tbl.IngestRecords("ingest")

	unloadTestTable(tableName)
	nt := GetTable(tableName)

	nt.LoadRecords(nil)

	if len(nt.RowBlock.RecordList) != CHUNK_SIZE*blockCount {
		t.Error("Row Store didn't read back right number of records", len(nt.RowBlock.RecordList))
	}

	if len(nt.BlockList) != 1 {
		t.Error("Found other records than rowblock")
	}

	querySpec := newQuerySpec()

	querySpec.Groups = append(querySpec.Groups, nt.Grouping("age_str"))
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation("age", "avg"))

	nt.MatchAndAggregate(querySpec)

	// Test that the group by and int keys are correctly re-assembled
	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		val, err := strconv.ParseInt(k, 10, 64)
		if err != nil || math.Abs(float64(val)-float64(v.Hists["age"].Mean())) > 0.1 {
			t.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, val, v.Hists["age"].Mean())
		}
	}

}
