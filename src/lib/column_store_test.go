package sybil

import (
	"math/rand"
	"strconv"
	"testing"
)

func TestTableDigestRowRecords(test *testing.T) {
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

	DELETE_BLOCKS_AFTER_QUERY = false
	FLAGS.READ_INGESTION_LOG = &TRUE

	nt.LoadTableInfo()
	nt.LoadRecords(nil)

	if len(nt.RowBlock.RecordList) != CHUNK_SIZE*blockCount {
		test.Error("Row Store didn't read back right number of records", len(nt.RowBlock.RecordList))
	}

	if len(nt.BlockList) != 1 {
		test.Error("Found other records than rowblock")
	}

	nt.DigestRecords()

	unloadTestTable()

	READ_ROWS_ONLY = false
	nt = GetTable(testTableName)
	nt.LoadRecords(nil)

	count := int32(0)
	for _, b := range nt.BlockList {
		Debug("COUNTING RECORDS IN", b.Name)
		count += b.Info.NumRecords
	}

	if count != int32(blockCount*CHUNK_SIZE) {
		test.Error("COLUMN STORE RETURNED TOO FEW COLUMNS", count)
	}

}
