package sybil

import (
	"fmt"
	"os"
	"testing"

	"github.com/logv/sybil/src/lib/common"
)

var testTableName = "__TEST0__"

type RecordSetupCB func(*Record, int)

func TestMain(m *testing.M) {
	runTests(m)
	deleteTestDB()
}

func runTests(m *testing.M) {
	setupTestVars(100)
	m.Run()
}

func setupTestVars(chunkSize int) {
	Startup()
	common.FLAGS.TABLE = &testTableName

	common.TEST_MODE = true
	CHUNK_SIZE = chunkSize
	LOCK_US = 1
	LOCK_TRIES = 3
}

func addRecordsToTestDB(cb RecordSetupCB, blockCount int) []*Record {
	count := CHUNK_SIZE * blockCount

	ret := make([]*Record, 0)
	t := GetTable(testTableName)

	for i := 0; i < count; i++ {
		r := t.NewRecord()
		cb(r, i)
		ret = append(ret, r)
	}

	return ret
}

func saveAndReloadTestTable(test *testing.T, expectedBlocks int) *Table {

	expectedCount := CHUNK_SIZE * expectedBlocks
	t := GetTable(testTableName)

	t.SaveRecordsToColumns()

	unloadTestTable()

	nt := GetTable(testTableName)
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

func newTestQuerySpec() *QuerySpec {

	filters := []Filter{}
	aggs := []Aggregation{}
	groupings := []Grouping{}

	querySpec := QuerySpec{QueryParams: QueryParams{Groups: groupings, Filters: filters, Aggregations: aggs}}

	return &querySpec
}

func unloadTestTable() {
	delete(LOADED_TABLES, testTableName)
}

func deleteTestDB() {
	os.RemoveAll(fmt.Sprintf("db/%s", testTableName))
	unloadTestTable()
}
