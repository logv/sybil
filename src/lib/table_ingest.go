package sybil

import (
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/logv/sybil/src/lib/common"
	"github.com/logv/sybil/src/lib/config"
)

// to ingest, make a new tmp file inside ingest/ (or append to an existing one)
// to digest, make a new STOMACHE_DIR tempdir and move all files from ingest/ into it

var READ_ROWS_ONLY = false
var MIN_FILES_TO_DIGEST = 0

func (t *Table) getNewIngestBlockName() (string, error) {
	common.Debug("GETTING INGEST BLOCK NAME", *config.FLAGS.DIR, "TABLE", t.Name)
	name, err := ioutil.TempDir(path.Join(*config.FLAGS.DIR, t.Name), "block")
	return name, err
}

func (t *Table) getNewCacheBlockFile() (*os.File, error) {
	common.Debug("GETTING CACHE BLOCK NAME", *config.FLAGS.DIR, "TABLE", t.Name)
	table_cache_dir := path.Join(*config.FLAGS.DIR, t.Name, CACHE_DIR)
	os.MkdirAll(table_cache_dir, 0755)

	// this info block needs to be moved once its finished being written to
	file, err := ioutil.TempFile(table_cache_dir, "info")
	return file, err
}

// Go through newRecords list and save all the new records out to a row store
func (t *Table) IngestRecords(blockname string) {
	common.Debug("KEY TABLE", t.KeyTable)
	common.Debug("KEY TYPES", t.KeyTypes)

	t.AppendRecordsToLog(t.newRecords[:], blockname)
	t.newRecords = make(RecordList, 0)
	t.SaveTableInfo("info")
	t.ReleaseRecords()

	t.MaybeCompactRecords()
}

// TODO: figure out how often we actually do a collation check by storing last
// collation inside a file somewhere
func (t *Table) CompactRecords() {
	config.FLAGS.READ_INGESTION_LOG = &config.TRUE
	READ_ROWS_ONLY = true
	DELETE_BLOCKS_AFTER_QUERY = false
	HOLD_MATCHES = true

	t.ResetBlockCache()
	t.DigestRecords()

}

// we compact if:
// we have over X files
// we have over X megabytes of data
// remember, there is no reason to actually read the data off disk
// until we decide to compact
func (t *Table) MaybeCompactRecords() {
	if *config.FLAGS.SKIP_COMPACT == true {
		return
	}

	if t.ShouldCompactRowStore(INGEST_DIR) {
		t.CompactRecords()
	}
}

var NO_MORE_BLOCKS = GROUP_DELIMITER

type AfterRowBlockLoad func(string, RecordList)

var FILE_DIGEST_THRESHOLD = 256
var KB = int64(1024)
var SIZE_DIGEST_THRESHOLD = int64(1024) * 2
var MAX_ROW_STORE_TRIES = 20

func (t *Table) ShouldCompactRowStore(digest string) bool {
	dirname := path.Join(*config.FLAGS.DIR, t.Name, digest)
	// if the row store dir does not exist, skip the whole function
	_, err := os.Stat(dirname)
	if os.IsNotExist(err) {
		return false
	}

	var file *os.File
	for i := 0; i < LOCK_TRIES; i++ {
		file, err = os.Open(dirname)
		if err != nil {
			common.Debug("Can't open the ingestion dir", dirname)
			time.Sleep(LOCK_US)
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
func (t *Table) LoadRowStoreRecords(digest string, after_block_load_cb AfterRowBlockLoad) {
	dirname := path.Join(*config.FLAGS.DIR, t.Name, digest)
	var err error

	// if the row store dir does not exist, skip the whole function
	_, err = os.Stat(dirname)
	if os.IsNotExist(err) {
		if after_block_load_cb != nil {
			after_block_load_cb(NO_MORE_BLOCKS, nil)
		}

		return
	}

	var file *os.File
	for i := 0; i < LOCK_TRIES; i++ {
		file, err = os.Open(dirname)
		if err != nil {
			common.Debug("Can't open the ingestion dir", dirname)
			time.Sleep(LOCK_US)
			if i > MAX_ROW_STORE_TRIES {
				return
			}
			continue
		}
		break
	}

	files, err := file.Readdir(0)
	if t.RowBlock == nil {
		t.RowBlock = &TableBlock{}
		(*t.RowBlock).RecordList = make(RecordList, 0)
		t.RowBlock.Info = &SavedColumnInfo{}
		t.block_m.Lock()
		t.BlockList[ROW_STORE_BLOCK] = t.RowBlock
		t.block_m.Unlock()
		t.RowBlock.Name = ROW_STORE_BLOCK
	}

	for _, file := range files {
		filename := file.Name()

		// we can open .gz files as well as regular .db files
		cname := strings.TrimRight(filename, GZIP_EXT)

		if strings.HasSuffix(cname, ".db") == false {
			continue
		}

		filename = path.Join(dirname, file.Name())

		records := t.LoadRecordsFromLog(filename)
		if after_block_load_cb != nil {
			after_block_load_cb(filename, records)
		}
	}

	if after_block_load_cb != nil {
		after_block_load_cb(NO_MORE_BLOCKS, nil)
	}

}

func LoadRowBlockCB(digestname string, records RecordList) {
	if digestname == NO_MORE_BLOCKS {
		return
	}

	t := GetTable(*config.FLAGS.TABLE)
	block := t.RowBlock

	if len(records) > 0 {
		block.RecordList = append(block.RecordList, records...)
		block.Info.NumRecords = int32(len(block.RecordList))
	}

}

var DELETE_BLOCKS = make([]string, 0)

func (t *Table) RestoreUningestedFiles() {
	if t.GrabDigestLock() == false {
		common.Debug("CANT RESTORE UNINGESTED RECORDS WITHOUT DIGEST LOCK")
		return
	}

	ingestdir := path.Join(*config.FLAGS.DIR, t.Name, INGEST_DIR)
	os.MkdirAll(ingestdir, 0777)

	digesting := path.Join(*config.FLAGS.DIR, t.Name)
	file, _ := os.Open(digesting)
	dirs, _ := file.Readdir(0)

	for _, dir := range dirs {
		if strings.HasPrefix(dir.Name(), STOMACHE_DIR) && dir.IsDir() {
			fname := path.Join(digesting, dir.Name())
			file, _ := os.Open(fname)
			files, _ := file.Readdir(0)
			for _, file := range files {
				common.Debug("RESTORING UNINGESTED FILE", file.Name())
				from := path.Join(fname, file.Name())
				to := path.Join(ingestdir, file.Name())
				err := RenameAndMod(from, to)
				if err != nil {
					common.Debug("COULDNT RESTORE UNINGESTED FILE", from, to, err)
				}
			}

			err := os.Remove(path.Join(digesting, dir.Name()))
			if err != nil {
				common.Debug("REMOVING STOMACHE FAILED!", err)
			}

		}
	}

}

type SaveBlockChunkCB struct {
	digestdir string
}

func (cb *SaveBlockChunkCB) CB(digestname string, records RecordList) {

	t := GetTable(*config.FLAGS.TABLE)
	if digestname == NO_MORE_BLOCKS {
		if len(t.newRecords) > 0 {
			t.SaveRecordsToColumns()
			t.ReleaseRecords()
		}

		for _, file := range DELETE_BLOCKS {
			common.Debug("REMOVING", file)
			os.Remove(file)
		}

		dir, err := os.Open(cb.digestdir)
		if err == nil {
			contents, err := dir.Readdir(0)

			if err == nil && len(contents) == 0 {
				os.RemoveAll(cb.digestdir)
			}
		}
		t.ReleaseDigestLock()
		return
	}

	common.Debug("LOADED", len(records), "FOR DIGESTION FROM", digestname)
	if len(records) > 0 {
		t.newRecords = append(t.newRecords, records...)
	}
	DELETE_BLOCKS = append(DELETE_BLOCKS, digestname)

}

var STOMACHE_DIR = "stomache"

// Go through rowstore and save records out to column store
func (t *Table) DigestRecords() {
	can_digest := t.GrabDigestLock()

	if !can_digest {
		t.ReleaseInfoLock()
		common.Debug("CANT GRAB LOCK FOR DIGEST RECORDS")
		return
	}

	dirname := path.Join(*config.FLAGS.DIR, t.Name)
	digestfile := path.Join(dirname, INGEST_DIR)
	digesting, err := ioutil.TempDir(dirname, STOMACHE_DIR)

	// TODO: we need to figure a way out such that the STOMACHE_DIR isn't going
	// to ruin us if it still exists (bc some proc didn't clean up after itself)
	if err != nil {
		t.ReleaseDigestLock()
		common.Debug("ERROR CREATING DIGESTION DIR", err)
		time.Sleep(time.Millisecond * 50)
		return
	}

	file, _ := os.Open(digestfile)

	files, err := file.Readdir(0)
	if len(files) < MIN_FILES_TO_DIGEST {
		common.Debug("SKIPPING DIGESTION, NOT AS MANY FILES AS WE THOUGHT", len(files), "VS", MIN_FILES_TO_DIGEST)
		t.ReleaseDigestLock()
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
		t.LoadRowStoreRecords(basename, cb.CB)
	} else {
		t.ReleaseDigestLock()
	}
}
