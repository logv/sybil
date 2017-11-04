package sybil

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	. "github.com/logv/sybil/src/lib/record"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/query/load_and_query"
	. "github.com/logv/sybil/src/query/specs"
	md_io "github.com/logv/sybil/src/storage/metadata_io"
)

func TestTableCreate(test *testing.T) {
	DeleteTestDB()

	blockCount := 3
	created := AddRecordsToTestDB(func(r *Record, index int) {
		AddIntField(r, "id", int64(index))
		age := int64(rand.Intn(20)) + 10
		AddIntField(r, "age", age)
		AddStrField(r, "ageStr", strconv.FormatInt(int64(age), 10))
		AddIntField(r, "time", int64(time.Now().Unix()))
		AddStrField(r, "name", fmt.Sprint("user", index))
	}, blockCount)

	nt := SaveAndReloadTestTable(test, blockCount)

	if nt.Name != TEST_TABLE_NAME {
		test.Error("TEST TABLE NAME INCORRECT")
	}

	md_io.LoadTableInfo(nt)

	_, err := os.Open(fmt.Sprintf("db/%s/info.db", TEST_TABLE_NAME))
	if err != nil {
		fmt.Println("ERR", err)
		test.Error("Test table did not create info.db")
	}

	loadSpec := NewTableLoadSpec(nt)
	loadSpec.LoadAllColumns = true

	LoadRecords(nt, &loadSpec)

	var records = make([]*Record, 0)
	for _, b := range nt.BlockList {
		records = append(records, b.RecordList...)
	}

	if len(records) != len(created) {
		test.Error("More records were created than expected", len(records))
	}

	DeleteTestDB()
}
