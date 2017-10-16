package sybil_test

import sybil "./"

import "strconv"
import "math/rand"
import "testing"
import "os"

func TestTableDigestRowRecords(test *testing.T) {
	deleteTestDb()

	blockCount := 3
	addRecords(func(r *sybil.Record, index int) {
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age)
		r.AddStrField("ageStr", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	t := sybil.GetTable(TestTableName)
	t.IngestRecords("ingest")

	unloadTestTable()
	nt := sybil.GetTable(TestTableName)
	sybil.DeleteBlocksAfterQuery = false
	sybil.FLAGS.ReadIngestionLog = &sybil.TRUE

	nt.LoadTableInfo()
	nt.LoadRecords(nil)

	if len(nt.RowBlock.RecordList) != sybil.ChunkSize*blockCount {
		test.Error("Row Store didn't read back right number of records", len(nt.RowBlock.RecordList))
	}

	if len(nt.BlockList) != 1 {
		test.Error("Found other records than rowblock")
	}

	nt.DigestRecords()

	unloadTestTable()

	sybil.ReadRowsOnly = false
	nt = sybil.GetTable(TestTableName)
	nt.LoadRecords(nil)

	count := int32(0)
	for _, b := range nt.BlockList {
		Debug("COUNTING RECORDS IN", b.Name)
		count += b.Info.NumRecords
	}

	if count != int32(blockCount*sybil.ChunkSize) {
		test.Error("COLUMN STORE RETURNED TOO FEW COLUMNS", count)

	}

}

func TestColumnStoreFileNames(test *testing.T) {

	deleteTestDb()

	blockCount := 3
	addRecords(func(r *sybil.Record, index int) {
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age)
		r.AddStrField("ageStr", strconv.FormatInt(int64(age), 10))
		r.AddSetField("ageSet", []string{strconv.FormatInt(int64(age), 10)})
	}, blockCount)

	t := sybil.GetTable(TestTableName)
	t.IngestRecords("ingest")

	unloadTestTable()
	nt := sybil.GetTable(TestTableName)
	sybil.DeleteBlocksAfterQuery = false
	sybil.FLAGS.ReadIngestionLog = &sybil.TRUE

	nt.LoadTableInfo()
	nt.LoadRecords(nil)

	if len(nt.RowBlock.RecordList) != sybil.ChunkSize*blockCount {
		test.Error("Row Store didn't read back right number of records", len(nt.RowBlock.RecordList))
	}

	if len(nt.BlockList) != 1 {
		test.Error("Found other records than rowblock")
	}

	nt.DigestRecords()

	unloadTestTable()

	sybil.ReadRowsOnly = false
	nt = sybil.GetTable(TestTableName)
	nt.LoadRecords(nil)

	count := int32(0)

	for _, b := range nt.BlockList {
		Debug("COUNTING RECORDS IN", b.Name)
		count += b.Info.NumRecords

		file, _ := os.Open(b.Name)
		files, _ := file.Readdir(-1)
		created_files := make(map[string]bool)

		for _, f := range files {
			created_files[f.Name()] = true
		}

		Debug("FILENAMES", created_files)
		Debug("BLOCK NAME", b.Name)
		if b.Name == sybil.RowStoreBlock {
			continue
		}

		var colFiles = []string{"int_age.db", "int_id.db", "str_ageStr.db", "set_ageSet.db"}
		for _, filename := range colFiles {
			_, ok := created_files[filename]
			if !ok {
				test.Error("MISSING COLUMN FILE", filename)
			}

		}

	}

	if count != int32(blockCount*sybil.ChunkSize) {
		test.Error("COLUMN STORE RETURNED TOO FEW COLUMNS", count)
	}

}
