package sybil

import (
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/structs"

	col_store "github.com/logv/sybil/src/storage/column_store"
	flock "github.com/logv/sybil/src/storage/file_locks"
	md_io "github.com/logv/sybil/src/storage/metadata_io"
)

// to ingest, make a new tmp file inside ingest/ (or append to an existing one)
// to digest, make a new STOMACHE_DIR tempdir and move all files from ingest/ into it

var MIN_FILES_TO_DIGEST = 0

// Go through NewRecords list and save all the new records out to a row store
func IngestRecords(t *Table, blockname string) {
	Debug("KEY TABLE", t.KeyTable)
	Debug("KEY TYPES", t.KeyTypes)

	AppendRecordsToLog(t, t.NewRecords[:], blockname)
	t.NewRecords = make(RecordList, 0)
	md_io.SaveTableInfo(t, "info")
	ReleaseRecords(t)

	MaybeCompactRecords(t)
}

// TODO: figure out how often we actually do a collation check by storing last
// collation inside a file somewhere
func CompactRecords(t *Table) {
	FLAGS.READ_INGESTION_LOG = &TRUE
	OPTS.READ_ROWS_ONLY = true
	OPTS.DELETE_BLOCKS_AFTER_QUERY = false
	OPTS.HOLD_MATCHES = true

	ResetBlockCache(t)
	DigestRecords(t)

}

// we compact if:
// we have over X files
// we have over X megabytes of data
// remember, there is no reason to actually read the data off disk
// until we decide to compact
func MaybeCompactRecords(t *Table) {
	Debug("CHECKING IF SHOULD COMPACT RECORDS")
	if *FLAGS.SKIP_COMPACT == true {
		return
	}

	if ShouldCompactRowStore(t, INGEST_DIR) {
		CompactRecords(t)
	}
}

var FILE_DIGEST_THRESHOLD = 256
var KB = int64(1024)
var SIZE_DIGEST_THRESHOLD = int64(1024) * 2

func ShouldCompactRowStore(t *Table, digest string) bool {
	dirname := path.Join(*FLAGS.DIR, t.Name, digest)
	// if the row store dir does not exist, skip the whole function
	_, err := os.Stat(dirname)
	if os.IsNotExist(err) {
		return false
	}

	var file *os.File
	for i := 0; i < flock.LOCK_TRIES; i++ {
		file, err = os.Open(dirname)
		if err != nil {
			Debug("Can't open the ingestion dir", dirname)
			time.Sleep(flock.LOCK_US)
			if i > MAX_ROW_STORE_TRIES {
				return false
			}

			continue
		}
		break
	}

	files, err := file.Readdir(0)
	MIN_FILES_TO_DIGEST = len(files)

	if len(files) > FILE_DIGEST_THRESHOLD {
		return true
	}

	size := int64(0)
	for _, f := range files {
		size = size + f.Size()
	}

	// compact every MB or so
	if size/KB > SIZE_DIGEST_THRESHOLD {
		return true
	}

	return false

}
func LoadRowBlockCB(digestname string, records RecordList) {
	if digestname == NO_MORE_BLOCKS {
		return
	}

	t := GetTable(*FLAGS.TABLE)
	block := t.RowBlock

	if len(records) > 0 {
		block.RecordList = append(block.RecordList, records...)
		block.Info.NumRecords = int32(len(block.RecordList))
	}

}

var DELETE_BLOCKS = make([]string, 0)

func RestoreUningestedFiles(t *Table) {
	if flock.GrabDigestLock(t) == false {
		Debug("CANT RESTORE UNINGESTED RECORDS WITHOUT DIGEST LOCK")
		return
	}

	ingestdir := path.Join(*FLAGS.DIR, t.Name, INGEST_DIR)
	os.MkdirAll(ingestdir, 0777)

	digesting := path.Join(*FLAGS.DIR, t.Name)
	file, _ := os.Open(digesting)
	dirs, _ := file.Readdir(0)

	for _, dir := range dirs {
		if strings.HasPrefix(dir.Name(), STOMACHE_DIR) && dir.IsDir() {
			fname := path.Join(digesting, dir.Name())
			file, _ := os.Open(fname)
			files, _ := file.Readdir(0)
			for _, file := range files {
				Debug("RESTORING UNINGESTED FILE", file.Name())
				from := path.Join(fname, file.Name())
				to := path.Join(ingestdir, file.Name())
				err := RenameAndMod(from, to)
				if err != nil {
					Debug("COULDNT RESTORE UNINGESTED FILE", from, to, err)
				}
			}

			err := os.Remove(path.Join(digesting, dir.Name()))
			if err != nil {
				Debug("REMOVING STOMACHE FAILED!", err)
			}

		}
	}

}

type SaveBlockChunkCB struct {
	digestdir string
}

func (cb *SaveBlockChunkCB) CB(digestname string, records RecordList) {

	t := GetTable(*FLAGS.TABLE)
	if digestname == NO_MORE_BLOCKS {
		if len(t.NewRecords) > 0 {
			col_store.SaveRecordsToColumns(t)
			ReleaseRecords(t)
		}

		for _, file := range DELETE_BLOCKS {
			Debug("REMOVING", file)
			os.Remove(file)
		}

		dir, err := os.Open(cb.digestdir)
		if err == nil {
			contents, err := dir.Readdir(0)

			if err == nil && len(contents) == 0 {
				os.RemoveAll(cb.digestdir)
			}
		}
		flock.ReleaseDigestLock(t)
		return
	}

	Debug("LOADED", len(records), "FOR DIGESTION FROM", digestname)
	if len(records) > 0 {
		t.NewRecords = append(t.NewRecords, records...)
	}
	DELETE_BLOCKS = append(DELETE_BLOCKS, digestname)

}

// Go through rowstore and save records out to column store
func DigestRecords(t *Table) {
	can_digest := flock.GrabDigestLock(t)

	if !can_digest {
		flock.ReleaseInfoLock(t)
		Debug("CANT GRAB LOCK FOR DIGEST RECORDS")
		return
	}

	dirname := path.Join(*FLAGS.DIR, t.Name)
	digestfile := path.Join(dirname, INGEST_DIR)
	digesting, err := ioutil.TempDir(dirname, STOMACHE_DIR)

	// TODO: we need to figure a way out such that the STOMACHE_DIR isn't going
	// to ruin us if it still exists (bc some proc didn't clean up after itself)
	if err != nil {
		flock.ReleaseDigestLock(t)
		Debug("ERROR CREATING DIGESTION DIR", err)
		time.Sleep(time.Millisecond * 50)
		return
	}

	file, _ := os.Open(digestfile)

	files, err := file.Readdir(0)
	if len(files) < MIN_FILES_TO_DIGEST {
		Debug("SKIPPING DIGESTION, NOT AS MANY FILES AS WE THOUGHT", len(files), "VS", MIN_FILES_TO_DIGEST)
		flock.ReleaseDigestLock(t)
		return
	}

	if err == nil {
		for _, f := range files {
			RenameAndMod(path.Join(digestfile, f.Name()), path.Join(digesting, f.Name()))
		}
		// We don't want to leave someone without a place to put their
		// ingestions...
		os.MkdirAll(digestfile, 0777)
		basename := path.Base(digesting)
		cb := SaveBlockChunkCB{digesting}
		LoadRowStoreRecords(t, basename, cb.CB)
	} else {
		flock.ReleaseDigestLock(t)
	}
}
