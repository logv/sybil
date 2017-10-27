package sybil

import (
	"fmt"
	"os"
	"testing"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/record"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/query/specs"
	. "github.com/logv/sybil/src/storage/column_store"
	. "github.com/logv/sybil/src/storage/file_locks"
	. "github.com/logv/sybil/src/query/load_and_query"
	. "github.com/logv/sybil/src/storage/metadata_io"
)

var TEST_TABLE_NAME = "__TEST0__"

type RecordSetupCB func(*Record, int)

func RunTests(m *testing.M) {
	DeleteTestDB()
	setupTestVars(100)
	m.Run()
}

func setupTestVars(chunkSize int) {
	PutLocksInTestMode()
	FLAGS.TABLE = &TEST_TABLE_NAME

	OPTS.DELETE_BLOCKS_AFTER_QUERY = false
	TEST_MODE = true
	CHUNK_SIZE = chunkSize

}

func AddRecordsToTestDB(cb RecordSetupCB, blockCount int) []*Record {
	count := CHUNK_SIZE * blockCount

	ret := make([]*Record, 0)
	t := GetTable(TEST_TABLE_NAME)

	for i := 0; i < count; i++ {
		r := NewRecord(t)
		cb(r, i)
		ret = append(ret, r)
	}

	return ret
}

func SaveAndReloadTestTable(test *testing.T, expectedBlocks int) *Table {

	expectedCount := CHUNK_SIZE * expectedBlocks
	t := GetTable(TEST_TABLE_NAME)

	SaveRecordsToColumns(t)

	UnloadTestTable()

	nt := GetTable(TEST_TABLE_NAME)
	LoadTableInfo(nt)

	loadSpec := NewLoadSpec()
	loadSpec.LoadAllColumns = true
	count := LoadRecords(nt, &loadSpec)

	if count != expectedCount {
		test.Error("Wrote", expectedCount, "records, but read back", count)
	}

	// +1 is the Row Store Block...
	if len(nt.BlockList) != expectedBlocks {
		test.Error("Wrote", expectedBlocks, "blocks, but came back with", len(nt.BlockList))
	}

	return nt

}

func NewTestQuerySpec() *QuerySpec {

	filters := []Filter{}
	aggs := []Aggregation{}
	groupings := []Grouping{}

	querySpec := QuerySpec{QueryParams: QueryParams{Groups: groupings, Filters: filters, Aggregations: aggs}}

	return &querySpec
}

func UnloadTestTable() {
	delete(LOADED_TABLES, TEST_TABLE_NAME)
}

func DeleteTestDB() {
	os.RemoveAll(fmt.Sprintf("db/%s", TEST_TABLE_NAME))
	UnloadTestTable()
}
