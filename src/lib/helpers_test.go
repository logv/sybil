package sybil_test

import sybil "./"
import "os"
import "fmt"
import "testing"

var TestTableName = "_Test0_"

var falseFlag = false
var trueFlag = true

// we copy over Debug from sybil package for usage
var Debug = sybil.Debug

type RecordSetupCB func(*sybil.Record, int)

func TestMain(m *testing.M) {
	runTests(m)
	deleteTestDb()
}

func runTests(m *testing.M) {
	setupTestVars(100)
	m.Run()
}

func setupTestVars(chunkSize int) {
	sybil.Startup()
	sybil.FLAGS.Table = &TestTableName

	sybil.TestMode = true
	sybil.ChunkSize = chunkSize
	sybil.LockUs = 1
	sybil.LockTries = 3
}

func addRecords(cb RecordSetupCB, blockCount int) []*sybil.Record {
	count := sybil.ChunkSize * blockCount

	ret := make([]*sybil.Record, 0)
	t := sybil.GetTable(TestTableName)

	for i := 0; i < count; i++ {
		r := t.NewRecord()
		cb(r, i)
		ret = append(ret, r)
	}

	return ret
}

func saveAndReloadTable(test *testing.T, expectedBlocks int) *sybil.Table {

	expectedCount := sybil.ChunkSize * expectedBlocks
	t := sybil.GetTable(TestTableName)

	t.SaveRecordsToColumns()

	unloadTestTable()

	nt := sybil.GetTable(TestTableName)
	nt.LoadTableInfo()

	loadSpec := sybil.NewLoadSpec()
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

func newQuerySpec() *sybil.QuerySpec {

	filters := []sybil.Filter{}
	aggs := []sybil.Aggregation{}
	groupings := []sybil.Grouping{}

	querySpec := sybil.QuerySpec{QueryParams: sybil.QueryParams{Groups: groupings, Filters: filters, Aggregations: aggs}}

	return &querySpec
}

func unloadTestTable() {
	delete(sybil.LoadedTables, TestTableName)
}

func deleteTestDb() {
	os.RemoveAll(fmt.Sprintf("db/%s", TestTableName))
	unloadTestTable()
}
