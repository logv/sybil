package pcs_test

import pcs "../"
import "os"
import "fmt"

var TEST_TABLE_NAME = "__TEST0__"

func unload_test_table() {
	delete(pcs.LOADED_TABLES, TEST_TABLE_NAME)
}

func delete_test_db() {
	os.RemoveAll(fmt.Sprintf("db/%s", TEST_TABLE_NAME))
	delete(pcs.LOADED_TABLES, TEST_TABLE_NAME)
}
