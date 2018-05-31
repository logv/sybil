package sybil

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
)

func TestTableDigestRowRecords(t *testing.T) {
	t.Parallel()
	flags := DefaultFlags()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)

	blockCount := 3
	addRecords(*flags.DIR, tableName, func(r *Record, index int) {
		r.AddIntField("id", int64(index), *flags.SKIP_OUTLIERS)
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age, *flags.SKIP_OUTLIERS)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	tbl := GetTable(*flags.DIR, tableName)
	tbl.IngestRecords(flags, "ingest")

	unloadTestTable(tableName)
	nt := GetTable(*flags.DIR, tableName)
	flags.TABLE = &tableName // TODO: eliminate global use

	nt.LoadTableInfo()
	nt.LoadRecords(&LoadSpec{
		SkipDeleteBlocksAfterQuery: true,
		ReadIngestionLog:           true,
	})

	if len(nt.RowBlock.RecordList) != CHUNK_SIZE*blockCount {
		t.Error("Row Store didn't read back right number of records", len(nt.RowBlock.RecordList))
	}

	if len(nt.BlockList) != 1 {
		t.Error("Found other records than rowblock")
	}

	nt.DigestRecords(flags)

	unloadTestTable(tableName)

	nt = GetTable(*flags.DIR, tableName)
	nt.LoadRecords(nil)

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
	t.Parallel()
	flags := DefaultFlags()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)

	blockCount := 3
	addRecords(*flags.DIR, tableName, func(r *Record, index int) {
		r.AddIntField("id", int64(index), *flags.SKIP_OUTLIERS)
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age, *flags.SKIP_OUTLIERS)
		r.AddStrField("ageStr", strconv.FormatInt(int64(age), 10))
		r.AddSetField("ageSet", []string{strconv.FormatInt(int64(age), 10)})
	}, blockCount)

	tbl := GetTable(*flags.DIR, tableName)
	tbl.IngestRecords(flags, "ingest")

	unloadTestTable(tableName)
	nt := GetTable(*flags.DIR, tableName)
	flags.TABLE = &tableName // TODO: eliminate global use

	nt.LoadTableInfo()
	nt.LoadRecords(&LoadSpec{
		SkipDeleteBlocksAfterQuery: true,
		ReadIngestionLog:           true,
	})

	if len(nt.RowBlock.RecordList) != CHUNK_SIZE*blockCount {
		t.Error("Row Store didn't read back right number of records", len(nt.RowBlock.RecordList))
	}

	if len(nt.BlockList) != 1 {
		t.Error("Found other records than rowblock")
	}

	nt.DigestRecords(flags)

	unloadTestTable(tableName)

	nt = GetTable(*flags.DIR, tableName)
	nt.LoadRecords(nil)

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
	t.Parallel()
	flags := DefaultFlags()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)

	var minVal = int64(1 << 50)
	blockCount := 3
	addRecords(*flags.DIR, tableName, func(r *Record, index int) {
		r.AddIntField("id", int64(index), *flags.SKIP_OUTLIERS)
		age := int64(rand.Intn(1 << 20))
		r.AddIntField("time", minVal+age, *flags.SKIP_OUTLIERS)
	}, blockCount)

	tbl := GetTable(*flags.DIR, tableName)
	tbl.IngestRecords(flags, "ingest")

	unloadTestTable(tableName)
	nt := GetTable(*flags.DIR, tableName)
	flags.TABLE = &tableName // TODO: eliminate global use

	nt.LoadTableInfo()
	nt.LoadRecords(&LoadSpec{
		SkipDeleteBlocksAfterQuery: true,
		ReadIngestionLog:           true,
	})

	if len(nt.RowBlock.RecordList) != CHUNK_SIZE*blockCount {
		t.Error("Row Store didn't read back right number of records", len(nt.RowBlock.RecordList))
	}

	if len(nt.BlockList) != 1 {
		t.Error("Found other records than rowblock")
	}

	nt.DigestRecords(flags)

	unloadTestTable(tableName)

	nt = GetTable(*flags.DIR, tableName)

	loadSpec := nt.NewLoadSpec()
	loadSpec.LoadAllColumns = true
	nt.LoadRecords(&loadSpec)

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
		t.Error("COLUMN STORE RETURNED TOO FEW COLUMNS", count)

	}

}
