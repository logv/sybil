package sybil

import "testing"

// Try out the different situations for lock recovery and see if they behave
// appropriately
func TestGrabInfoLock(t *testing.T) {
	t.Parallel()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)
	tbl := GetTable(tableName)

	tbl.MakeDir()

	if err := tbl.GrabInfoLock(); err != nil {
		t.Errorf("COULD NOT GRAB INFO LOCK, tried %v - %v", tableName, err)
	}
}

func TestRecoverInfoLock(t *testing.T) {
	t.Parallel()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)
	tbl := GetTable(tableName)
	tbl.MakeDir()
	lock := Lock{Table: tbl, Name: "info"}
	lock.ForceMakeFile(int64(0))
	infolock := InfoLock{lock}

	tbl.MakeDir()

	grabbed := tbl.GrabInfoLock() == nil
	if grabbed {
		t.Error("GRABBED INFO LOCK WHEN IT ALREADY EXISTS AND BELONGS ELSEWHERE")
	}

	infolock.Recover()

}

func TestGrabDigestLock(t *testing.T) {
	t.Parallel()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)
	tbl := GetTable(tableName)

	tbl.MakeDir()
	if err := tbl.GrabDigestLock(); err != nil {
		t.Error("COULD NOT GRAB DIGEST LOCK", err)
	}
}

func TestRecoverDigestLock(t *testing.T) {
	t.Parallel()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)
	tbl := GetTable(tableName)
	tbl.MakeDir()

	// first grab digest lock
	if err := tbl.GrabDigestLock(); err != nil {
		t.Error("COULD NOT GRAB DIGEST LOCK", err)
	}

	lock := Lock{Table: tbl, Name: STOMACHE_DIR}
	lock.ForceMakeFile(int64(0))

	tbl.MakeDir()
	grabbed := tbl.GrabDigestLock() == nil
	if grabbed {
		t.Error("COULD GRAB DIGEST LOCK WHEN IT ARLEADY EXISTS")
	}
}
