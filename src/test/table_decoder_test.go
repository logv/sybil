package sybil

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/lib/record"
	. "github.com/logv/sybil/src/query/load_and_query"
	. "github.com/logv/sybil/src/query/specs"
	. "github.com/logv/sybil/src/storage/metadata_io"
	. "github.com/logv/sybil/src/storage/row_store"
)

func TestOpenCompressedInfoDB(test *testing.T) {
	DeleteTestDB()

	blockCount := 3
	created := AddRecordsToTestDB(func(r *Record, index int) {
		AddIntField(r, "id", int64(index))
		age := int64(rand.Intn(20)) + 10
		AddIntField(r, "age", age)
		AddStrField(r, "ageStr", strconv.FormatInt(int64(age), 10))
		AddIntField(r, "time", int64(time.Now().Unix()))
		AddStrField(r, "name", fmt.Sprint("user", index))
	}, blockCount)

	nt := SaveAndReloadTestTable(test, blockCount)

	if nt.Name != TEST_TABLE_NAME {
		test.Error("TEST TABLE NAME INCORRECT")
	}

	filename := fmt.Sprintf("db/%s/info.db", TEST_TABLE_NAME)
	dat, err := ioutil.ReadFile(filename)

	if err != nil {
		fmt.Println("ERR", err)
		test.Error("Test table did not create info.db")
	}

	// NOW WE COMPRESS INFO.DB.GZ
	zfilename := fmt.Sprintf("db/%s/info.db.gz", TEST_TABLE_NAME)
	file, err := os.Create(zfilename)
	if err != nil {
		test.Error("COULDNT LOAD ZIPPED TABLE FILE FOR WRITING!")

	}
	zinfo := gzip.NewWriter(file)
	zinfo.Write(dat)
	zinfo.Close()

	os.RemoveAll(filename)
	// END ZIPPING INFO.DB.GZ

	loadSpec := NewTableLoadSpec(nt)
	loadSpec.LoadAllColumns = true

	loaded := LoadTableInfo(nt)
	if loaded == false {
		test.Error("COULDNT LOAD ZIPPED TABLE INFO!")
	}

	LoadRecords(nt, &loadSpec)

	var records = make([]*Record, 0)
	for _, b := range nt.BlockList {
		records = append(records, b.RecordList...)
	}

	if len(records) != len(created) {
		test.Error("More records were created than expected", len(records))
	}

	DeleteTestDB()

}

func TestOpenCompressedColumn(test *testing.T) {
	DeleteTestDB()

	blockCount := 3
	created := AddRecordsToTestDB(func(r *Record, index int) {
		AddIntField(r, "id", int64(index))
		age := int64(rand.Intn(20)) + 10
		AddIntField(r, "age", age)
		AddStrField(r, "ageStr", strconv.FormatInt(int64(age), 10))
		AddIntField(r, "time", int64(time.Now().Unix()))
		AddStrField(r, "name", fmt.Sprint("user", index))
	}, blockCount)

	nt := SaveAndReloadTestTable(test, blockCount)
	DigestRecords(nt)
	LoadRecords(nt, nil)

	blocks := nt.BlockList

	if nt.Name != TEST_TABLE_NAME {
		test.Error("TEST TABLE NAME INCORRECT")
	}

	// NOW WE COMPRESS ALL THE BLOCK FILES BY ITERATING THROUGH THE DIR AND
	// DOING SO
	for blockname := range blocks {
		files, _ := ioutil.ReadDir(blockname)
		Debug("READING BLOCKNAME", blockname)
		for _, f := range files {
			filename := path.Join(blockname, f.Name())
			if !strings.HasSuffix(filename, ".db") {
				continue
			}
			dat, _ := ioutil.ReadFile(filename)

			zfilename := fmt.Sprintf("%s.gz", filename)
			file, err := os.Create(zfilename)
			if err != nil {
				test.Error("COULDNT LOAD ZIPPED TABLE FILE FOR WRITING!")
			}
			zinfo := gzip.NewWriter(file)
			zinfo.Write(dat)
			zinfo.Close()
			Debug("CREATED GZIP FILE", zfilename)

			err = os.RemoveAll(filename)
			Debug("REMOVED", filename, err)

		}
	}

	// END COMPRESSING BLOCK FILES

	bt := SaveAndReloadTestTable(test, blockCount)

	loadSpec := NewTableLoadSpec(bt)
	loadSpec.LoadAllColumns = true

	loaded := LoadTableInfo(bt)
	if loaded == false {
		test.Error("COULDNT LOAD ZIPPED TABLE INFO!")
	}

	LoadRecords(bt, &loadSpec)

	var records = make([]*Record, 0)
	for _, b := range bt.BlockList {
		records = append(records, b.RecordList...)
	}

	if len(records) != len(created) {
		test.Error("More records were created than expected", len(records))
	}

	DeleteTestDB()

}
