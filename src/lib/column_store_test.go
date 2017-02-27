package sybil_test

import sybil "./"

import "strconv"
import "math/rand"
import "testing"


func TestTableDigestRowRecords(test *testing.T) {
	delete_test_db()

	block_count := 3
	add_records(func(r *sybil.Record, index int) {
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, block_count)

	t := sybil.GetTable(TEST_TABLE_NAME)
	t.IngestRecords("ingest")

	unload_test_table()
	nt := sybil.GetTable(TEST_TABLE_NAME)
	sybil.DELETE_BLOCKS_AFTER_QUERY = false
	sybil.FLAGS.READ_INGESTION_LOG = &sybil.TRUE

	nt.LoadTableInfo()
	nt.LoadRecords(nil)

	if len(nt.RowBlock.RecordList) != sybil.CHUNK_SIZE*block_count {
		test.Error("Row Store didn't read back right number of records", len(nt.RowBlock.RecordList))
	}

	if len(nt.BlockList) != 1 {
		test.Error("Found other records than rowblock")
	}

	nt.DigestRecords()

	unload_test_table()

	sybil.READ_ROWS_ONLY = false
	nt = sybil.GetTable(TEST_TABLE_NAME)
	nt.LoadRecords(nil)

	count := int32(0)
	for _, b := range nt.BlockList {
		Debug("COUNTING RECORDS IN", b.Name)
		count += b.Info.NumRecords
	}

	if count != int32(block_count*sybil.CHUNK_SIZE) {
		test.Error("COLUMN STORE RETURNED TOO FEW COLUMNS", count)

	}

}
