package sybil

import "testing"

// Try out the different situations for lock recovery and see if they behave
// appropriately
func TestGrabInfoLock(t *testing.T) {
	t.Parallel()
	flags := DefaultFlags()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)
	tbl := GetTable(tableName)

	tbl.MakeDir(flags)

	grabbed := tbl.GrabInfoLock(flags)
	if !grabbed {
		t.Errorf("COULD NOT GRAB INFO LOCK, tried %v", tableName)
	}
}

func TestRecoverInfoLock(t *testing.T) {
	t.Parallel()
	flags := DefaultFlags()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)
	tbl := GetTable(tableName)
	tbl.MakeDir(flags)
	lock := Lock{Table: tbl, Name: "info"}
	lock.ForceMakeFile(flags, int64(0))
	infolock := InfoLock{lock}

	tbl.MakeDir(flags)

	grabbed := tbl.GrabInfoLock(flags)
	if grabbed {
		t.Error("GRABBED INFO LOCK WHEN IT ALREADY EXISTS AND BELONGS ELSEWHERE")
	}

	infolock.Recover(flags)

}

func TestGrabDigestLock(t *testing.T) {
	t.Parallel()
	flags := DefaultFlags()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)
	tbl := GetTable(tableName)

	tbl.MakeDir(flags)
	grabbed := tbl.GrabDigestLock(flags)
	if !grabbed {
		t.Error("COULD NOT GRAB DIGEST LOCK")
	}
}

func TestRecoverDigestLock(t *testing.T) {
	t.Parallel()
	flags := DefaultFlags()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)
	tbl := GetTable(tableName)
	tbl.MakeDir(flags)

	// first grab digest lock
	if grabbed := tbl.GrabDigestLock(flags); !grabbed {
		t.Error("COULD NOT GRAB DIGEST LOCK")
	}

	lock := Lock{Table: tbl, Name: STOMACHE_DIR}
	lock.ForceMakeFile(flags, int64(0))

	tbl.MakeDir(flags)
	grabbed := tbl.GrabDigestLock(flags)
	if grabbed {
		t.Error("COULD GRAB DIGEST LOCK WHEN IT ARLEADY EXISTS")
	}
}
