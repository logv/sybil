package sybil_test

import sybil "./"

import "testing"

// Try out the different situations for lock recovery and see if they behave
// appropriately
func TestGrabInfoLock(test *testing.T) {
	t := sybil.GetTable(TestTableName)

	t.MakeDir()

	grabbed := t.GrabInfoLock()
	if grabbed != true {
		test.Error("COULD NOT GRAB INFO LOCK")
	}
}

func TestRecoverInfoLock(test *testing.T) {
	t := sybil.GetTable(TestTableName)
	lock := sybil.Lock{Table: t, Name: "info"}
	lock.ForceMakeFile(int64(0))
	infolock := sybil.InfoLock{lock}

	t.MakeDir()

	grabbed := t.GrabInfoLock()
	if grabbed == true {
		test.Error("GRABBED INFO LOCK WHEN IT ALREADY EXISTS AND BELONGS ELSEWHERE")
	}

	infolock.Recover()

}

func TestGrabDigestLock(test *testing.T) {
	t := sybil.GetTable(TestTableName)

	t.MakeDir()
	grabbed := t.GrabDigestLock()
	if grabbed != true {
		test.Error("COULD NOT GRAB DIGEST LOCK")
	}
}

func TestRecoverDigestLock(test *testing.T) {
	t := sybil.GetTable(TestTableName)
	lock := sybil.Lock{Table: t, Name: sybil.StomacheDir}
	lock.ForceMakeFile(int64(0))

	t.MakeDir()
	grabbed := t.GrabDigestLock()
	if grabbed == true {
		test.Error("COULD GRAB DIGEST LOCK WHEN IT ARLEADY EXISTS")
	}

}
