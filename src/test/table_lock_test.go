package sybil

import "testing"

import (
	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/storage/file_locks"
)

// Try out the different situations for lock recovery and see if they behave
// appropriately
func TestGrabInfoLock(test *testing.T) {
	PutLocksInTestMode()
	Debug("LOCK US", LOCK_US, "LOCK TRIES", LOCK_TRIES)
	t := GetTable(TEST_TABLE_NAME)

	MakeDir(t)

	grabbed := GrabInfoLock(t)
	if grabbed != true {
		test.Error("COULD NOT GRAB INFO LOCK")
	}
}

func TestGrabDigestLock(test *testing.T) {
	PutLocksInTestMode()
	t := GetTable(TEST_TABLE_NAME)

	MakeDir(t)
	grabbed := GrabDigestLock(t)
	if grabbed != true {
		test.Error("COULD NOT GRAB DIGEST LOCK")
	}
}
