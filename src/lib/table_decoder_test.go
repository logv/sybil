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
)

func TestOpenCompressedInfoDB(test *testing.T) {
	deleteTestDB()

	blockCount := 3
	created := addRecordsToTestDB(func(r *Record, index int) {
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age)
		r.AddStrField("ageStr", strconv.FormatInt(int64(age), 10))
		r.AddIntField("time", int64(time.Now().Unix()))
		r.AddStrField("name", fmt.Sprint("user", index))
	}, blockCount)

	nt := saveAndReloadTestTable(test, blockCount)

	if nt.Name != testTableName {
		test.Error("TEST TABLE NAME INCORRECT")
	}

	filename := fmt.Sprintf("db/%s/info.db", testTableName)
	dat, err := ioutil.ReadFile(filename)

	if err != nil {
		fmt.Println("ERR", err)
		test.Error("Test table did not create info.db")
	}

	// NOW WE COMPRESS INFO.DB.GZ
	zfilename := fmt.Sprintf("db/%s/info.db.gz", testTableName)
	file, err := os.Create(zfilename)
	if err != nil {
		test.Error("COULDNT LOAD ZIPPED TABLE FILE FOR WRITING!")

	}
	zinfo := gzip.NewWriter(file)
	zinfo.Write(dat)
	zinfo.Close()

	os.RemoveAll(filename)
	// END ZIPPING INFO.DB.GZ

	loadSpec := nt.NewLoadSpec()
	loadSpec.LoadAllColumns = true

	loaded := nt.LoadTableInfo()
	if loaded == false {
		test.Error("COULDNT LOAD ZIPPED TABLE INFO!")
	}

	nt.LoadRecords(&loadSpec)

	var records = make([]*Record, 0)
	for _, b := range nt.BlockList {
		records = append(records, b.RecordList...)
	}

	if len(records) != len(created) {
		test.Error("More records were created than expected", len(records))
	}

	deleteTestDB()

}

func TestOpenCompressedColumn(test *testing.T) {
	deleteTestDB()

	blockCount := 3
	created := addRecordsToTestDB(func(r *Record, index int) {
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age)
		r.AddStrField("ageStr", strconv.FormatInt(int64(age), 10))
		r.AddIntField("time", int64(time.Now().Unix()))
		r.AddStrField("name", fmt.Sprint("user", index))
	}, blockCount)

	nt := saveAndReloadTestTable(test, blockCount)
	nt.DigestRecords()
	nt.LoadRecords(nil)

	blocks := nt.BlockList

	if nt.Name != testTableName {
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

	bt := saveAndReloadTestTable(test, blockCount)

	loadSpec := bt.NewLoadSpec()
	loadSpec.LoadAllColumns = true

	loaded := bt.LoadTableInfo()
	if loaded == false {
		test.Error("COULDNT LOAD ZIPPED TABLE INFO!")
	}

	bt.LoadRecords(&loadSpec)

	var records = make([]*Record, 0)
	for _, b := range bt.BlockList {
		records = append(records, b.RecordList...)
	}

	if len(records) != len(created) {
		test.Error("More records were created than expected", len(records))
	}

	deleteTestDB()

}
