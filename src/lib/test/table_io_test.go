package pcs_test

import pcs "../"

import "testing"
import "fmt"
import "time"
import "os"

func TestTableCreate(test *testing.T) {
	delete_test_db()
	unload_test_table()

	t := pcs.GetTable(TEST_TABLE_NAME)

	fmt.Println("TABLE", t)

	if t.Name != TEST_TABLE_NAME {
		test.Error("TEST TABLE NAME INCORRECT")
	}
	r := t.NewRecord()

	r.AddIntField("age", 10)
	r.AddIntField("time", int64(time.Now().Unix()))
	r.AddStrField("name", "user1")

	t.SaveTableInfo("info")
	t.SaveRecords()

	unload_test_table()

	nt := pcs.GetTable(TEST_TABLE_NAME)
	fmt.Println("KEY TABLE", nt.KeyTable)
	nt.LoadTableInfo()

	_, err := os.Open(fmt.Sprintf("db/%s/info.db", TEST_TABLE_NAME))
	if err != nil {
		fmt.Println("ERR", err)
		test.Error("Test table did not create info.db")
	}

	loadSpec := pcs.NewLoadSpec()
	loadSpec.LoadAllColumns = true

	nt.LoadRecords(&loadSpec)
	pcs.SetLoadAndQuery(false)

	var records = make([]*pcs.Record, 0)
	for _, b := range nt.BlockList {
		records = append(records, b.RecordList...)
	}

	if len(records) != 1 {
		test.Error("More records were created than expected", len(records))
	}

	delete_test_db()
}
