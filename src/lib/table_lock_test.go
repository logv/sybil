package sybil

import "testing"

// Try out the different situations for lock recovery and see if they behave
// appropriately
func TestGrabInfoLock(t *testing.T) {
	tbl := GetTable(TEST_TABLE_NAME)

	tbl.MakeDir()

	grabbed := tbl.GrabInfoLock()
	if grabbed != true {
		t.Error("COULD NOT GRAB INFO LOCK")
	}
}

func TestRecoverInfoLock(t *testing.T) {
	tbl := GetTable(TEST_TABLE_NAME)
	lock := Lock{Table: tbl, Name: "info"}
	lock.ForceMakeFile(int64(0))
	infolock := InfoLock{lock}

	tbl.MakeDir()

	grabbed := tbl.GrabInfoLock()
	if grabbed == true {
		t.Error("GRABBED INFO LOCK WHEN IT ALREADY EXISTS AND BELONGS ELSEWHERE")
	}

	infolock.Recover()

}

func TestGrabDigestLock(t *testing.T) {
	tbl := GetTable(TEST_TABLE_NAME)

	tbl.MakeDir()
	grabbed := tbl.GrabDigestLock()
	if grabbed != true {
		t.Error("COULD NOT GRAB DIGEST LOCK")
	}
}

func TestRecoverDigestLock(t *testing.T) {
	tbl := GetTable(TEST_TABLE_NAME)
	lock := Lock{Table: tbl, Name: STOMACHE_DIR}
	lock.ForceMakeFile(int64(0))

	tbl.MakeDir()
	grabbed := tbl.GrabDigestLock()
	if grabbed == true {
		t.Error("COULD GRAB DIGEST LOCK WHEN IT ARLEADY EXISTS")
	}
}
