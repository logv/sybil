package sybil

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/storage/file_locks"

	col_store "github.com/logv/sybil/src/storage/column_store"
	encoders "github.com/logv/sybil/src/storage/encoders"
	md_io "github.com/logv/sybil/src/storage/metadata_io"
	row_store "github.com/logv/sybil/src/storage/row_store"
)

func MultiLockRecover(l RecoverableLock) bool {
	switch v := l.(type) {
	case *InfoLock:
		return RecoverInfoLock(v)
	case *CacheLock:
		return RecoverCacheLock(v)
	case *BlockLock:
		return RecoverBlockLock(v)
	case *DigestLock:
		return RecoverDigestLock(v)
	default:
		fmt.Println("UNKNOWN LOCK TYPE", l)
	}

	return false

}

func RecoverInfoLock(l *InfoLock) bool {
	t := l.Lock.Table
	dirname := path.Join(*FLAGS.DIR, t.Name)
	backup := path.Join(dirname, "info.bak")
	infodb := path.Join(dirname, "info.db")

	if md_io.LoadTableInfoFrom(t, infodb) {
		Debug("LOADED REASONABLE TABLE INFO, DELETING LOCK")
		l.ForceDeleteFile()
		return true
	}

	if md_io.LoadTableInfoFrom(t, backup) {
		Debug("LOADED TABLE INFO FROM BACKUP, RESTORING BACKUP")
		os.Remove(infodb)
		RenameAndMod(backup, infodb)
		l.ForceDeleteFile()
		return l.Grab()
	}

	Debug("CANT READ info.db OR RECOVER info.bak")
	Debug("TRY DELETING LOCK BY HAND FOR", l.Name)

	return false
}

func RecoverDigestLock(l *DigestLock) bool {
	Debug("RECOVERING DIGEST LOCK", l.Name)
	t := l.Table
	ingestdir := path.Join(*FLAGS.DIR, t.Name, INGEST_DIR)

	os.MkdirAll(ingestdir, 0777)
	// TODO: understand if any file in particular is messing things up...
	pid := int64(os.Getpid())
	l.ForceMakeFile(pid)
	row_store.RestoreUningestedFiles(t)
	l.ForceDeleteFile()

	return true
}

func RecoverBlockLock(l *BlockLock) bool {
	Debug("RECOVERING BLOCK LOCK", l.Name)
	t := l.Table
	tb := col_store.LoadBlockFromDir(t, l.Name, nil, true)
	if tb == nil || tb.Info == nil || tb.Info.NumRecords <= 0 {
		Debug("BLOCK IS NO GOOD, TURNING IT INTO A BROKEN BLOCK")
		// This block is not good! need to put it into remediation...
		RenameAndMod(l.Name, fmt.Sprint(l.Name, ".broke"))
		l.ForceDeleteFile()
	} else {
		Debug("BLOCK IS FINE, TURNING IT BACK INTO A REAL BLOCK")
		os.RemoveAll(fmt.Sprint(l.Name, ".partial"))
		l.ForceDeleteFile()
	}

	return true
}

func RecoverCacheLock(l *CacheLock) bool {
	Debug("RECOVERING BLOCK LOCK", l.Name)
	t := l.Table
	files, err := ioutil.ReadDir(path.Join(*FLAGS.DIR, t.Name, CACHE_DIR))

	if err != nil {
		l.ForceDeleteFile()
		return true
	}

	for _, block_file := range files {
		filename := path.Join(*FLAGS.DIR, t.Name, CACHE_DIR, block_file.Name())
		block_cache := col_store.SavedBlockCache{}

		err := encoders.DecodeInto(filename, &block_cache)
		if err != nil {
			os.RemoveAll(filename)
			continue
		}

		if err != nil {
			os.RemoveAll(filename)
			Debug("DELETING BAD CACHE FILE", filename)

		}

	}

	l.ForceDeleteFile()

	return true

}

func InjectLockRecoveryHandlers() {
	SetRecoverFunction(MultiLockRecover)
}

func init() {
	InjectLockRecoveryHandlers()
}
