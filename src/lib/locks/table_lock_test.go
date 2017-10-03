package locks

import "testing"

import (
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/lib/table"
	. "github.com/logv/sybil/src/lib/test_helpers"
)

func TestMain(m *testing.M) {
	RunTests(m)
	DeleteTestDB()
}

// Try out the different situations for lock recovery and see if they behave
// appropriately
func TestGrabInfoLock(test *testing.T) {
	t := GetTable(TEST_TABLE_NAME)

	MakeDir(t)

	grabbed := GrabInfoLock(t)
	if grabbed != true {
		test.Error("COULD NOT GRAB INFO LOCK")
	}
}

func TestRecoverInfoLock(test *testing.T) {
	t := GetTable(TEST_TABLE_NAME)
	lock := Lock{Table: t, Name: "info"}
	lock.ForceMakeFile(int64(0))
	infolock := InfoLock{lock}

	MakeDir(t)

	grabbed := GrabInfoLock(t)
	if grabbed == true {
		test.Error("GRABBED INFO LOCK WHEN IT ALREADY EXISTS AND BELONGS ELSEWHERE")
	}

	infolock.Recover()

}

func TestGrabDigestLock(test *testing.T) {
	t := GetTable(TEST_TABLE_NAME)

	MakeDir(t)
	grabbed := GrabDigestLock(t)
	if grabbed != true {
		test.Error("COULD NOT GRAB DIGEST LOCK")
	}
}

func TestRecoverDigestLock(test *testing.T) {
	t := GetTable(TEST_TABLE_NAME)
	lock := Lock{Table: t, Name: STOMACHE_DIR}
	lock.ForceMakeFile(int64(0))

	MakeDir(t)
	grabbed := GrabDigestLock(t)
	if grabbed == true {
		test.Error("COULD GRAB DIGEST LOCK WHEN IT ARLEADY EXISTS")
	}

}
