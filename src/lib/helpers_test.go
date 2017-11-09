package sybil

import "os"
import "fmt"
import "testing"

var TEST_TABLE_NAME = "__TEST0__"

type RecordSetupCB func(*Record, int)

func TestMain(m *testing.M) {
	run_tests(m)
	delete_test_db()
}

func run_tests(m *testing.M) {
	setup_test_vars(100)
	m.Run()
}

var BLANK_STRING = ""

func setup_test_vars(chunk_size int) {
	FLAGS.TABLE = &TEST_TABLE_NAME
	FLAGS.OP = &BLANK_STRING

	TEST_MODE = true
	CHUNK_SIZE = chunk_size
	LOCK_US = 1
	LOCK_TRIES = 3
}

func add_records(cb RecordSetupCB, block_count int) []*Record {
	count := CHUNK_SIZE * block_count

	ret := make([]*Record, 0)
	t := GetTable(TEST_TABLE_NAME)

	for i := 0; i < count; i++ {
		r := t.NewRecord()
		cb(r, i)
		ret = append(ret, r)
	}

	return ret
}

func save_and_reload_table(test *testing.T, expected_blocks int) *Table {

	expected_count := CHUNK_SIZE * expected_blocks
	t := GetTable(TEST_TABLE_NAME)

	t.SaveRecordsToColumns()

	unload_test_table()

	nt := GetTable(TEST_TABLE_NAME)
	nt.LoadTableInfo()

	loadSpec := NewLoadSpec()
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

func new_query_spec() *QuerySpec {

	filters := []Filter{}
	aggs := []Aggregation{}
	groupings := []Grouping{}

	querySpec := QuerySpec{QueryParams: QueryParams{Groups: groupings, Filters: filters, Aggregations: aggs}}

	return &querySpec
}

func unload_test_table() {
	delete(LOADED_TABLES, TEST_TABLE_NAME)
}

func delete_test_db() {
	os.RemoveAll(fmt.Sprintf("db/%s", TEST_TABLE_NAME))
	unload_test_table()
}
