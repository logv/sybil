package sybil

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
)

type RecordSetupCB func(*Record, int)

func TestMain(m *testing.M) {
	runTests(m)
}

func runTests(m *testing.M) {
	setupTestVars(100)
	os.Exit(m.Run())
}

func setupTestVars(chunkSize int) {
	TEST_MODE = true
	CHUNK_SIZE = chunkSize
	LOCK_US = 1
	LOCK_TRIES = 3
}

// getTestTableName uses the caller as the test name to help provide test case isolation.
func getTestTableName(t *testing.T) string {
	fpcs := make([]uintptr, 1)
	n := runtime.Callers(2, fpcs)
	if n == 0 {
		return "default"
	}
	fun := runtime.FuncForPC(fpcs[0] - 1)
	if fun == nil {
		return "default"
	}
	parts := strings.Split(fun.Name(), ".")
	return parts[len(parts)-1]
}

func addRecords(tableName string, cb RecordSetupCB, blockCount int) []*Record {
	count := CHUNK_SIZE * blockCount

	ret := make([]*Record, 0)
	tbl := GetTable(tableName)

	for i := 0; i < count; i++ {
		r := tbl.NewRecord()
		cb(r, i)
		ret = append(ret, r)
	}

	return ret
}

func saveAndReloadTable(t *testing.T, tableName string, expectedBlocks int) *Table {
	expectedCount := CHUNK_SIZE * expectedBlocks
	tbl := GetTable(tableName)

	tbl.SaveRecordsToColumns()

	unloadTestTable(tableName)

	nt := GetTable(tableName)
	nt.LoadTableInfo()

	loadSpec := NewLoadSpec()
	loadSpec.LoadAllColumns = true
	count, err := nt.LoadRecords(&loadSpec)
	if err != nil {
		t.Fatal(err)
	}

	if count != expectedCount {
		t.Error("Wrote", expectedCount, "records, but read back", count)
	}

	// +1 is the Row Store Block...
	if len(nt.BlockList) != expectedBlocks {
		t.Error("Wrote", expectedBlocks, "blocks, but came back with", len(nt.BlockList))
	}

	return nt

}

func newQuerySpec() *QuerySpec {

	filters := []Filter{}
	aggs := []Aggregation{}
	groupings := []Grouping{}

	querySpec := QuerySpec{
		QueryParams: QueryParams{
			Groups:       groupings,
			Filters:      filters,
			Aggregations: aggs,
		},
	}

	return &querySpec
}

func unloadTestTable(tableName string) {
	UnloadTable(tableName)
}

func deleteTestDb(tableName string) {
	os.RemoveAll(fmt.Sprintf("db/%s", tableName))
	unloadTestTable(tableName)
}
