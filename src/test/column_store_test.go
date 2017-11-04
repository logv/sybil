package sybil

import (
	"math/rand"
	"os"
	"strconv"
	"testing"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/record"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/query/load_and_query"
	. "github.com/logv/sybil/src/query/specs"
	md_io "github.com/logv/sybil/src/storage/metadata_io"
	row_store "github.com/logv/sybil/src/storage/row_store"
)

func TestTableDigestRowRecords(test *testing.T) {
	DeleteTestDB()

	blockCount := 3
	AddRecordsToTestDB(func(r *Record, index int) {
		AddIntField(r, "id", int64(index))
		age := int64(rand.Intn(20)) + 10
		AddIntField(r, "age", age)
		AddStrField(r, "ageStr", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	t := GetTable(TEST_TABLE_NAME)
	row_store.IngestRecords(t, "ingest")

	UnloadTestTable()
	nt := GetTable(TEST_TABLE_NAME)

	OPTS.DELETE_BLOCKS_AFTER_QUERY = false
	FLAGS.READ_INGESTION_LOG = &TRUE

	md_io.LoadTableInfo(nt)
	LoadRecords(nt, nil)

	if len(nt.RowBlock.RecordList) != CHUNK_SIZE*blockCount {
		test.Error("Row Store didn't read back right number of records", len(nt.RowBlock.RecordList))
	}

	if len(nt.BlockList) != 1 {
		test.Error("Found other records than rowblock")
	}

	row_store.DigestRecords(nt)

	UnloadTestTable()

	OPTS.READ_ROWS_ONLY = false
	nt = GetTable(TEST_TABLE_NAME)
	LoadRecords(nt, nil)

	count := int32(0)
	for _, b := range nt.BlockList {
		Debug("COUNTING RECORDS IN", b.Name)
		count += b.Info.NumRecords
	}

	if count != int32(blockCount*CHUNK_SIZE) {
		test.Error("COLUMN STORE RETURNED TOO FEW COLUMNS", count)
	}

}

func TestColumnStoreFileNames(test *testing.T) {

	DeleteTestDB()

	blockCount := 3
	AddRecordsToTestDB(func(r *Record, index int) {
		AddIntField(r, "id", int64(index))
		age := int64(rand.Intn(20)) + 10
		AddIntField(r, "age", age)
		AddStrField(r, "ageStr", strconv.FormatInt(int64(age), 10))
		AddSetField(r, "ageSet", []string{strconv.FormatInt(int64(age), 10)})
	}, blockCount)

	t := GetTable(TEST_TABLE_NAME)
	row_store.IngestRecords(t, "ingest")

	UnloadTestTable()
	nt := GetTable(TEST_TABLE_NAME)
	OPTS.DELETE_BLOCKS_AFTER_QUERY = false
	FLAGS.READ_INGESTION_LOG = &TRUE

	md_io.LoadTableInfo(nt)
	LoadRecords(nt, nil)

	if len(nt.RowBlock.RecordList) != CHUNK_SIZE*blockCount {
		test.Error("Row Store didn't read back right number of records", len(nt.RowBlock.RecordList))
	}

	if len(nt.BlockList) != 1 {
		test.Error("Found other records than rowblock")
	}

	row_store.DigestRecords(nt)

	UnloadTestTable()

	OPTS.READ_ROWS_ONLY = false
	nt = GetTable(TEST_TABLE_NAME)
	LoadRecords(nt, nil)

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
		if b.Name == ROW_STORE_BLOCK {
			continue
		}

		// TODO: add test to make sure previous version filenames still work, too
		// in fact, make a function to generate filenames: func(name, type, ext)
		var colFiles = []string{"age.int.gob", "id.int.gob", "ageStr.str.gob", "ageSet.set.gob"}
		for _, filename := range colFiles {
			_, ok := created_files[filename]
			if !ok {
				test.Error("MISSING COLUMN FILE", filename)
				Debug("FILES ARE", colFiles)
			}

		}

	}

	if count != int32(blockCount*CHUNK_SIZE) {
		test.Error("COLUMN STORE RETURNED TOO FEW COLUMNS", count)
	}

}

func TestBigIntColumns(test *testing.T) {
	DeleteTestDB()

	var minVal = int64(1 << 50)
	blockCount := 3
	AddRecordsToTestDB(func(r *Record, index int) {
		AddIntField(r, "id", int64(index))
		age := int64(rand.Intn(1 << 20))
		AddIntField(r, "time", minVal+age)
	}, blockCount)

	t := GetTable(TEST_TABLE_NAME)
	row_store.IngestRecords(t, "ingest")

	UnloadTestTable()
	nt := GetTable(TEST_TABLE_NAME)
	OPTS.DELETE_BLOCKS_AFTER_QUERY = false
	FLAGS.READ_INGESTION_LOG = &TRUE

	md_io.LoadTableInfo(nt)
	LoadRecords(nt, nil)

	if len(nt.RowBlock.RecordList) != CHUNK_SIZE*blockCount {
		test.Error("Row Store didn't read back right number of records", len(nt.RowBlock.RecordList))
	}

	if len(nt.BlockList) != 1 {
		test.Error("Found other records than rowblock")
	}

	row_store.DigestRecords(nt)

	UnloadTestTable()

	OPTS.READ_ROWS_ONLY = false
	FLAGS.SAMPLES = &TRUE
	limit := 1000
	FLAGS.LIMIT = &limit
	nt = GetTable(TEST_TABLE_NAME)

	loadSpec := NewTableLoadSpec(nt)
	loadSpec.LoadAllColumns = true
	LoadRecords(nt, &loadSpec)

	count := int32(0)
	Debug("MIN VALUE BEING CHECKED FOR IS", minVal, "2^32 is", 1<<32)
	Debug("MIN VAL IS BIGGER?", minVal > 1<<32)
	for _, b := range nt.BlockList {
		Debug("VERIFYING BIG INTS IN", b.Name)
		for _, r := range b.RecordList {
			v, ok := GetIntVal(r, "time")
			if int64(v) < minVal || !ok {
				test.Error("BIG INT UNPACKED INCORRECTLY! VAL:", v, "OK?", ok)
			}

		}
		count += b.Info.NumRecords
	}

	if count != int32(blockCount*CHUNK_SIZE) {
		test.Error("COLUMN STORE RETURNED TOO FEW COLUMNS", count)

	}
	FLAGS.SAMPLES = &FALSE

}
