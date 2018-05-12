package sybil

import "os"
import "fmt"
import "testing"

var TEST_TABLE_NAME = "__TEST0__"

type RecordSetupCB func(*Record, int)

func TestMain(m *testing.M) {
	runTests(m)
	deleteTestDb()
}

func runTests(m *testing.M) {
	setupTestVars(100)
	m.Run()
}

var BLANK_STRING = ""

func setupTestVars(chunkSize int) {
	FLAGS.TABLE = &TEST_TABLE_NAME
	FLAGS.OP = &BLANK_STRING

	TEST_MODE = true
	CHUNK_SIZE = chunkSize
	LOCK_US = 1
	LOCK_TRIES = 3
}

func addRecords(cb RecordSetupCB, blockCount int) []*Record {
	count := CHUNK_SIZE * blockCount

	ret := make([]*Record, 0)
	t := GetTable(TEST_TABLE_NAME)

	for i := 0; i < count; i++ {
		r := t.NewRecord()
		cb(r, i)
		ret = append(ret, r)
	}

	return ret
}

func saveAndReloadTable(test *testing.T, expectedBlocks int) *Table {

	expectedCount := CHUNK_SIZE * expectedBlocks
	t := GetTable(TEST_TABLE_NAME)

	t.SaveRecordsToColumns()

	unloadTestTable()

	nt := GetTable(TEST_TABLE_NAME)
	nt.LoadTableInfo()

	loadSpec := NewLoadSpec()
	loadSpec.LoadAllColumns = true
	count := nt.LoadRecords(&loadSpec)

	if count != expectedCount {
		test.Error("Wrote", expectedCount, "records, but read back", count)
	}

	// +1 is the Row Store Block...
	if len(nt.BlockList) != expectedBlocks {
		test.Error("Wrote", expectedBlocks, "blocks, but came back with", len(nt.BlockList))
	}

	return nt

}

func newQuerySpec() *QuerySpec {

	filters := []Filter{}
	aggs := []Aggregation{}
	groupings := []Grouping{}

	querySpec := QuerySpec{QueryParams: QueryParams{Groups: groupings, Filters: filters, Aggregations: aggs}}

	return &querySpec
}

func unloadTestTable() {
	delete(LOADED_TABLES, TEST_TABLE_NAME)
}

func deleteTestDb() {
	os.RemoveAll(fmt.Sprintf("db/%s", TEST_TABLE_NAME))
	unloadTestTable()
}
