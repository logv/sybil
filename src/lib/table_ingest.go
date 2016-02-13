package sybil

import "path"
import "log"
import "os"
import "strings"

// there exists two dirs for ingesting and digesting:
// ingest/
// digest/

// to ingest, make a new tmp file inside ingest/ (or append to an existing one)
// to digest, move that file into stomache/ and begin digesting it

// Go through newRecords list and save all the new records out to a row store
func (t *Table) IngestRecords(blockname string) {
	log.Println("KEY TABLE", t.KeyTable)

	t.AppendRecordsToLog(t.newRecords[:], blockname)
	t.newRecords = make(RecordList, 0)
	t.SaveTableInfo("info")
}

var NO_MORE_BLOCKS = GROUP_DELIMITER

type AfterRowBlockLoad func(string, RecordList)

func (t *Table) LoadRowStoreRecords(digest string, after_block_load_cb AfterRowBlockLoad) {
	// TODO: REFUSE TO DIGEST IF THE DIGEST AREA ALREADY EXISTS
	dirname := path.Join(*f_DIR, t.Name, INGEST_DIR)

	file, err := os.Open(dirname)
	if err != nil {
		log.Println("Can't open the ingestion dir", dirname)
		return
	}

	if t.RowBlock == nil {
		t.RowBlock = &TableBlock{}
		(*t.RowBlock).RecordList = make(RecordList, 0)
		t.RowBlock.Info = &SavedColumnInfo{}
		t.BlockList[ROW_STORE_BLOCK] = t.RowBlock
		t.RowBlock.Name = ROW_STORE_BLOCK
	}

	files, err := file.Readdir(0)

	for _, file := range files {
		filename := file.Name()

		if strings.HasPrefix(filename, digest) == false {
			continue
		}
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

	t := GetTable(*f_TABLE)
	block := t.RowBlock

	if len(records) > 0 {
		block.RecordList = append(block.RecordList, records...)
		block.Info.NumRecords = int32(len(block.RecordList))
	}

}

func SaveBlockChunkCB(digestname string, records RecordList) {

	t := GetTable(*f_TABLE)
	if digestname == NO_MORE_BLOCKS {
		if len(t.newRecords) > 0 {
			t.SaveRecords()
			t.ReleaseRecords()
		}

		return
	}

	log.Println("LOADED", len(records), "FOR DIGESTION FROM", digestname)
	if len(records) > 0 {
		t.newRecords = append(t.newRecords, records...)
	}

	if len(t.newRecords) > 1000 {
		t.SaveRecords()
		t.ReleaseRecords()
	}

	log.Println("Removing", digestname)
	os.Remove(digestname)
}

// Go through rowstore and save records out to column store
func (t *Table) DigestRecords(digest string) {
	t.LoadRowStoreRecords(digest, SaveBlockChunkCB)
}
