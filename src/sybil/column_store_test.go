package sybil

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
)

func TestTableDigestRowRecords(t *testing.T) {
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)

	blockCount := 3
	addRecords(tableName, func(r *Record, index int) {
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	tbl := GetTable(tableName)
	tbl.IngestRecords("ingest")

	unloadTestTable(tableName)
	nt := GetTable(tableName)
	FLAGS.TABLE = tableName // TODO: eliminate global use
	FLAGS.READ_INGESTION_LOG = true

	if err := nt.LoadTableInfo(); err != nil {
		t.Error(err)
	}
	_, err := nt.LoadRecords(&LoadSpec{
		SkipDeleteBlocksAfterQuery: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(nt.RowBlock.RecordList) != CHUNK_SIZE*blockCount {
		t.Error("Row Store didn't read back right number of records", len(nt.RowBlock.RecordList))
	}

	if len(nt.BlockList) != 1 {
		t.Error("Found other records than rowblock")
	}

	if err := nt.DigestRecords(); err != nil {
		t.Error(err)
	}

	unloadTestTable(tableName)

	nt = GetTable(tableName)
	if _, err := nt.LoadRecords(nil); err != nil {
		t.Error(err)
	}

	count := int32(0)
	for _, b := range nt.BlockList {
		Debug("COUNTING RECORDS IN", b.Name)
		count += b.Info.NumRecords
	}

	if count != int32(blockCount*CHUNK_SIZE) {
		t.Errorf("COLUMN STORE RETURNED TOO FEW COLUMNS, got %v, want %v", count, blockCount*CHUNK_SIZE)

	}

}

func TestColumnStoreFileNames(t *testing.T) {
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)

	blockCount := 3
	addRecords(tableName, func(r *Record, index int) {
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age)
		r.AddStrField("ageStr", strconv.FormatInt(int64(age), 10))
		r.AddSetField("ageSet", []string{strconv.FormatInt(int64(age), 10)})
	}, blockCount)

	tbl := GetTable(tableName)
	tbl.IngestRecords("ingest")

	unloadTestTable(tableName)
	nt := GetTable(tableName)
	FLAGS.TABLE = tableName // TODO: eliminate global use
	FLAGS.READ_INGESTION_LOG = true

	if err := nt.LoadTableInfo(); err != nil {
		t.Error(err)
	}
	_, err := nt.LoadRecords(&LoadSpec{
		SkipDeleteBlocksAfterQuery: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(nt.RowBlock.RecordList) != CHUNK_SIZE*blockCount {
		t.Error("Row Store didn't read back right number of records", len(nt.RowBlock.RecordList))
	}

	if len(nt.BlockList) != 1 {
		t.Error("Found other records than rowblock")
	}

	if err := nt.DigestRecords(); err != nil {
		t.Error(err)
	}

	unloadTestTable(tableName)

	nt = GetTable(tableName)
	if _, err := nt.LoadRecords(nil); err != nil {
		t.Error(err)
	}

	count := int32(0)

	for _, b := range nt.BlockList {
		Debug("COUNTING RECORDS IN", b.Name)
		count += b.Info.NumRecords

		file, _ := os.Open(b.Name)
		files, _ := file.Readdir(-1)
		createdFiles := make(map[string]bool)

		for _, f := range files {
			createdFiles[f.Name()] = true
		}

		Debug("FILENAMES", createdFiles)
		Debug("BLOCK NAME", b.Name)
		if b.Name == ROW_STORE_BLOCK {
			continue
		}

		var colFiles = []string{"int_age.db", "int_id.db", "str_ageStr.db", "set_ageSet.db"}
		for _, filename := range colFiles {
			_, ok := createdFiles[filename]
			if !ok {
				t.Error("MISSING COLUMN FILE", filename)
			}

		}

	}

	if count != int32(blockCount*CHUNK_SIZE) {
		t.Error("COLUMN STORE RETURNED TOO FEW COLUMNS", count)
	}

}

func TestBigIntColumns(t *testing.T) {
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)

	var minVal = int64(1 << 50)
	blockCount := 3
	addRecords(tableName, func(r *Record, index int) {
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(1 << 20))
		r.AddIntField("time", minVal+age)
	}, blockCount)

	tbl := GetTable(tableName)
	tbl.IngestRecords("ingest")

	unloadTestTable(tableName)
	nt := GetTable(tableName)
	FLAGS.TABLE = tableName // TODO: eliminate global use
	FLAGS.READ_INGESTION_LOG = true

	if err := nt.LoadTableInfo(); err != nil {
		t.Error(err)
	}
	_, err := nt.LoadRecords(&LoadSpec{
		SkipDeleteBlocksAfterQuery: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(nt.RowBlock.RecordList) != CHUNK_SIZE*blockCount {
		t.Error("Row Store didn't read back right number of records", len(nt.RowBlock.RecordList))
	}

	if len(nt.BlockList) != 1 {
		t.Error("Found other records than rowblock")
	}

	if err := nt.DigestRecords(); err != nil {
		t.Error(err)
	}

	unloadTestTable(tableName)

	nt = GetTable(tableName)

	loadSpec := nt.NewLoadSpec()
	loadSpec.LoadAllColumns = true
	querySpec := newQuerySpec()
	querySpec.Samples = true
	querySpec.Limit = 1000
	if _, err := nt.LoadAndQueryRecords(&loadSpec, querySpec); err != nil {
		t.Error(err)
	}

	count := int32(0)
	Debug("MIN VALUE BEING CHECKED FOR IS", minVal, "2^32 is", 1<<32)
	Debug("MIN VAL IS BIGGER?", minVal > 1<<32)
	for _, b := range nt.BlockList {
		Debug("VERIFYING BIG INTS IN", b.Name)
		for _, r := range b.RecordList {
			v, ok := r.GetIntVal("time")
			if int64(v) < minVal || !ok {
				t.Error("BIG INT UNPACKED INCORRECTLY! VAL:", v, "OK?", ok)
			}

		}
		count += b.Info.NumRecords
	}

	if count != int32(blockCount*CHUNK_SIZE) {
		t.Error("COLUMN STORE RETURNED TOO FEW COLUMNS", count, "vs", blockCount*CHUNK_SIZE)

	}

}
