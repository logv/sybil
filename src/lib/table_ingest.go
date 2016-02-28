package sybil

import "time"
import "path"
import "io/ioutil"
import "log"
import "os"
import "strings"

// to ingest, make a new tmp file inside ingest/ (or append to an existing one)
// to digest, make a new STOMACHE_DIR tempdir and move all files from ingest/ into it

var READ_ROWS_ONLY = false

func (t *Table) getNewIngestBlockName() (string, error) {
	log.Println("GETTING INGEST BLOCK NAME", *FLAGS.DIR, "TABLE", t.Name)
	name, err := ioutil.TempDir(path.Join(*FLAGS.DIR, t.Name), "block")
	return name, err
}

// Go through newRecords list and save all the new records out to a row store
func (t *Table) IngestRecords(blockname string) {
	log.Println("KEY TABLE", t.KeyTable)
	log.Println("KEY TYPES", t.KeyTypes)

	t.AppendRecordsToLog(t.newRecords[:], blockname)
	t.newRecords = make(RecordList, 0)
	t.SaveTableInfo("info")
	t.ReleaseRecords()

	FLAGS.READ_INGESTION_LOG = &TRUE
	READ_ROWS_ONLY = true
	DELETE_BLOCKS_AFTER_QUERY = false
	HOLD_MATCHES = true
	loaded := t.LoadRecords(nil)
	if loaded > 0 && t.RowBlock != nil && len(t.RowBlock.RecordList) > CHUNK_THRESHOLD {
		log.Println("LOADED RECORDS", len(t.RowBlock.RecordList))
		t.DigestRecords()
	}
}

var NO_MORE_BLOCKS = GROUP_DELIMITER

type AfterRowBlockLoad func(string, RecordList)

func (t *Table) LoadRowStoreRecords(digest string, after_block_load_cb AfterRowBlockLoad) {
	// TODO: REFUSE TO DIGEST IF THE DIGEST AREA ALREADY EXISTS
	dirname := path.Join(*FLAGS.DIR, t.Name, digest)

	os.MkdirAll(dirname, 0777)

	var file *os.File
	var err error
	for i := 0; i < LOCK_TRIES; i++ {
		file, err = os.Open(dirname)
		if err != nil {
			log.Println("Can't open the ingestion dir", dirname)
			time.Sleep(LOCK_US)
			continue
		}
	}

	files, err := file.Readdir(0)

	if t.RowBlock == nil {
		t.RowBlock = &TableBlock{}
		(*t.RowBlock).RecordList = make(RecordList, 0)
		t.RowBlock.Info = &SavedColumnInfo{}
		t.BlockList[ROW_STORE_BLOCK] = t.RowBlock
		t.RowBlock.Name = ROW_STORE_BLOCK
	}

	for _, file := range files {
		filename := file.Name()

		if strings.HasSuffix(filename, ".db") == false {
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
		log.Println("CANT RESTORE UNINGESTED RECORDS WITHOUT DIGEST LOCK")
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
				log.Println("RESTORING UNINGESTED FILE", file.Name())
				from := path.Join(fname, file.Name())
				to := path.Join(ingestdir, file.Name())
				err := os.Rename(from, to)
				if err != nil {
					log.Println("COULDNT RESTORE UNINGESTED FILE", from, to, err)
				}
			}

			err := os.Remove(path.Join(digesting, dir.Name()))
			if err != nil {
				log.Println("REMOVING STOMACHE FAILED!", err)
			}

		}
	}

}

func SaveBlockChunkCB(digestname string, records RecordList) {

	t := GetTable(*FLAGS.TABLE)
	if digestname == NO_MORE_BLOCKS {
		if len(t.newRecords) > 0 {
			t.SaveRecordsToColumns()
			t.ReleaseRecords()
		}

		for _, file := range DELETE_BLOCKS {
			log.Println("REMOVING", file)
			os.Remove(file)
		}

		t.ReleaseDigestLock()
		return
	}

	log.Println("LOADED", len(records), "FOR DIGESTION FROM", digestname)
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
		log.Println("CANT GRAB LOCK FOR DIGEST RECORDS")
		return
	}

	dirname := path.Join(*FLAGS.DIR, t.Name)
	digestfile := path.Join(dirname, INGEST_DIR)
	digesting, err := ioutil.TempDir(dirname, STOMACHE_DIR)

	// TODO: we need to figure a way out such that the STOMACHE_DIR isn't going
	// to ruin us if it still exists (bc some proc didn't clean up after itself)
	if err != nil {
		t.ReleaseDigestLock()
		log.Println("ERROR CREATING DIGESTION DIR", err)
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
		t.LoadRowStoreRecords(basename, SaveBlockChunkCB)
	} else {
		t.ReleaseDigestLock()
	}
}
