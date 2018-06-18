package sybil

import "testing"

func TestIngestion(t *testing.T) {
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)

}
