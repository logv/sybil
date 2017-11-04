package sybil

import "testing"

import (
	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/storage/file_locks"
	. "github.com/logv/sybil/src/storage/file_locks_recover"
)

func TestRecoverInfoLock(test *testing.T) {
	PutLocksInTestMode()
	t := GetTable(TEST_TABLE_NAME)
	MakeDir(t)
	lock := Lock{Table: t, Name: "info"}
	lock.ForceMakeFile(int64(0))

	grabbed := GrabInfoLock(t)
	if grabbed == true {
		test.Error("GRABBED INFO LOCK WHEN IT ALREADY EXISTS AND BELONGS ELSEWHERE")
	}

	infolock := InfoLock{lock}
	MultiLockRecover(&infolock)

}

func TestRecoverDigestLock(test *testing.T) {
	PutLocksInTestMode()
	t := GetTable(TEST_TABLE_NAME)
	lock := Lock{Table: t, Name: STOMACHE_DIR}
	lock.ForceMakeFile(int64(0))

	MakeDir(t)
	grabbed := GrabDigestLock(t)
	if grabbed == true {
		test.Error("COULD GRAB DIGEST LOCK WHEN IT ARLEADY EXISTS")
	}

}
