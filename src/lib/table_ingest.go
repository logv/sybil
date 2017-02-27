package sybil

import "time"
import "path"
import "io/ioutil"

import "os"
import "strings"

// to ingest, make a new tmp file inside ingest/ (or append to an existing one)
// to digest, make a new STOMACHE_DIR tempdir and move all files from ingest/ into it

var READ_ROWS_ONLY = false

func (t *Table) getNewIngestBlockName() (string, error) {
	Debug("GETTING INGEST BLOCK NAME", *FLAGS.DIR, "TABLE", t.Name)
	name, err := ioutil.TempDir(path.Join(*FLAGS.DIR, t.Name), "block")
	return name, err
}

func (t *Table) getNewCacheBlockFile() (*os.File, error) {
	Debug("GETTING CACHE BLOCK NAME", *FLAGS.DIR, "TABLE", t.Name)
	table_cache_dir := path.Join(*FLAGS.DIR, t.Name, CACHE_DIR)
	os.MkdirAll(table_cache_dir, 0755)

	// this info block needs to be moved once its finished being written to
	file, err := ioutil.TempFile(table_cache_dir, "info")
	return file, err
}

// Go through newRecords list and save all the new records out to a row store
func (t *Table) IngestRecords(blockname string) {
	Debug("KEY TABLE", t.KeyTable)
	Debug("KEY TYPES", t.KeyTypes)

	t.AppendRecordsToLog(t.newRecords[:], blockname)
	t.newRecords = make(RecordList, 0)
	t.SaveTableInfo("info")
	t.ReleaseRecords()
}

// TODO: figure out how often we actually do a collation check by storing last
// collation inside a file somewhere
func (t *Table) MaybeCompactRecords() {
	FLAGS.READ_INGESTION_LOG = &TRUE
	READ_ROWS_ONLY = true
	DELETE_BLOCKS_AFTER_QUERY = false
	HOLD_MATCHES = true
	loaded := t.LoadRecords(nil)
	if loaded > 0 && t.RowBlock != nil && len(t.RowBlock.RecordList) > CHUNK_THRESHOLD {
		Debug("LOADED RECORDS", len(t.RowBlock.RecordList))
		t.DigestRecords()
	}
}

var NO_MORE_BLOCKS = GROUP_DELIMITER

type AfterRowBlockLoad func(string, RecordList)

func (t *Table) LoadRowStoreRecords(digest string, after_block_load_cb AfterRowBlockLoad) {
	dirname := path.Join(*FLAGS.DIR, t.Name, digest)
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
			Debug("Can't open the ingestion dir", dirname)
			time.Sleep(LOCK_US)
			continue
		}
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

	t := GetTable(*FLAGS.TABLE)
	block := t.RowBlock

	if len(records) > 0 {
		block.RecordList = append(block.RecordList, records...)
		block.Info.NumRecords = int32(len(block.RecordList))
	}

}

var DELETE_BLOCKS = make([]string, 0)

func (t *Table) RestoreUningestedFiles() {
	if t.GrabDigestLock() == false {
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
				err := os.Rename(from, to)
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
		if len(t.newRecords) > 0 {
			t.SaveRecordsToColumns()
			t.ReleaseRecords()
		}

		for _, file := range DELETE_BLOCKS {
			Debug("REMOVING", file)
			os.Remove(file)
		}

		os.RemoveAll(cb.digestdir)
		t.ReleaseDigestLock()
		return
	}

	Debug("LOADED", len(records), "FOR DIGESTION FROM", digestname)
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
		Debug("CANT GRAB LOCK FOR DIGEST RECORDS")
		return
	}

	dirname := path.Join(*FLAGS.DIR, t.Name)
	digestfile := path.Join(dirname, INGEST_DIR)
	digesting, err := ioutil.TempDir(dirname, STOMACHE_DIR)

	// TODO: we need to figure a way out such that the STOMACHE_DIR isn't going
	// to ruin us if it still exists (bc some proc didn't clean up after itself)
	if err != nil {
		t.ReleaseDigestLock()
		Debug("ERROR CREATING DIGESTION DIR", err)
		time.Sleep(time.Millisecond * 50)
		return
	}

	file, _ := os.Open(digestfile)

	files, err := file.Readdir(0)
	if err == nil {
		for _, f := range files {
			os.Rename(path.Join(digestfile, f.Name()), path.Join(digesting, f.Name()))
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
