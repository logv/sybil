package sybil_test

import sybil "./"
import "os"
import "fmt"
import "testing"

var TEST_TABLE_NAME = "__TEST0__"

// we copy over Debug from sybil package for usage
var Debug = sybil.Debug

type RecordSetupCB func(*sybil.Record, int)

func TestMain(m *testing.M) {
	run_tests(m)
	delete_test_db()
}

func run_tests(m *testing.M) {
	setup_test_vars(100)
	m.Run()
}

func setup_test_vars(chunk_size int) {
	sybil.SetDefaults()
	sybil.FLAGS.TABLE = &TEST_TABLE_NAME

	sybil.TEST_MODE = true
	sybil.CHUNK_SIZE = chunk_size
	sybil.LOCK_US = 1
	sybil.LOCK_TRIES = 3
}

func add_records(cb RecordSetupCB, block_count int) []*sybil.Record {
	count := sybil.CHUNK_SIZE * block_count

	ret := make([]*sybil.Record, 0)
	t := sybil.GetTable(TEST_TABLE_NAME)

	for i := 0; i < count; i++ {
		r := t.NewRecord()
		cb(r, i)
		ret = append(ret, r)
	}

	return ret
}

func save_and_reload_table(test *testing.T, expected_blocks int) *sybil.Table {

	expected_count := sybil.CHUNK_SIZE * expected_blocks
	t := sybil.GetTable(TEST_TABLE_NAME)

	t.SaveRecordsToColumns()

	unload_test_table()

	nt := sybil.GetTable(TEST_TABLE_NAME)
	nt.LoadTableInfo()

	loadSpec := sybil.NewLoadSpec()
	loadSpec.LoadAllColumns = true
	count := nt.LoadRecords(&loadSpec)

	if count != expected_count {
		test.Error("Wrote", expected_count, "records, but read back", count)
	}

	// +1 is the Row Store Block...
	if len(nt.BlockList) != expected_blocks {
		test.Error("Wrote", expected_blocks, "blocks, but came back with", len(nt.BlockList))
	}

	return nt

}

func new_query_spec() *sybil.QuerySpec {

	filters := []sybil.Filter{}
	aggs := []sybil.Aggregation{}
	groupings := []sybil.Grouping{}

	querySpec := sybil.QuerySpec{Groups: groupings, Filters: filters, Aggregations: aggs}

	return &querySpec
}

func unload_test_table() {
	delete(sybil.LOADED_TABLES, TEST_TABLE_NAME)
}

func delete_test_db() {
	os.RemoveAll(fmt.Sprintf("db/%s", TEST_TABLE_NAME))
	unload_test_table()
}
