package edb_test

import edb "../"

import "testing"
import "fmt"
import "time"
import "os"

var TEST_TABLE_NAME = "__TEST0__"

func unload_test_table() {
	delete(edb.LOADED_TABLES, TEST_TABLE_NAME)
}

func delete_test_db() {
	os.RemoveAll(fmt.Sprintf("db/%s", TEST_TABLE_NAME))
}

func TestTableCreate(test *testing.T) {
	delete_test_db()

	t := edb.GetTable(TEST_TABLE_NAME)

	fmt.Println("TABLE", t)

	if t.Name != TEST_TABLE_NAME {
		test.Error("TEST TABLE NAME INCORRECT")
	}
	r := t.NewRecord()

	r.AddIntField("age", 10)
	r.AddIntField("time", int(time.Now().Unix()))
	r.AddStrField("name", "user1")

	t.SaveTableInfo("info")
	t.SaveRecords()

	unload_test_table()

	nt := edb.GetTable(TEST_TABLE_NAME)
	fmt.Println("KEY TABLE", nt.KeyTable)
	nt.LoadTableInfo()

	_, err := os.Open(fmt.Sprintf("db/%s/info.db", TEST_TABLE_NAME))
	if err != nil {
		fmt.Println("ERR", err)
		test.Error("Test table did not create info.db")
	}

	loadSpec := edb.NewLoadSpec()
	loadSpec.LoadAllColumns = true

	nt.LoadRecords(&loadSpec)

	var records = make([]*edb.Record, 0)
	for _, b := range nt.BlockList {
		records = append(records, b.RecordList...)
	}

	if len(records) != 1 {
		test.Error("More records were created than expected")
	}

	delete_test_db()
}
