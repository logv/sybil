package sybil_test

import sybil "./"

import "testing"
import "fmt"
import "time"
import "os"
import "math/rand"
import "strconv"

func TestTableCreate(test *testing.T) {
	deleteTestDb()

	blockCount := 3
	created := addRecords(func(r *sybil.Record, index int) {
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age)
		r.AddStrField("ageStr", strconv.FormatInt(int64(age), 10))
		r.AddIntField("time", int64(time.Now().Unix()))
		r.AddStrField("name", fmt.Sprint("user", index))
	}, blockCount)

	nt := saveAndReloadTable(test, blockCount)

	if nt.Name != TestTableName {
		test.Error("TEST TABLE NAME INCORRECT")
	}

	nt.LoadTableInfo()

	_, err := os.Open(fmt.Sprintf("db/%s/info.db", TestTableName))
	if err != nil {
		fmt.Println("ERR", err)
		test.Error("Test table did not create info.db")
	}

	loadSpec := nt.NewLoadSpec()
	loadSpec.LoadAllColumns = true

	nt.LoadRecords(&loadSpec)

	var records = make([]*sybil.Record, 0)
	for _, b := range nt.BlockList {
		records = append(records, b.RecordList...)
	}

	if len(records) != len(created) {
		test.Error("More records were created than expected", len(records))
	}

	deleteTestDb()
}
