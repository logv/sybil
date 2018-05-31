package sybil

import "compress/gzip"
import "fmt"
import "path"

import "io/ioutil"
import "math/rand"
import "os"
import "strconv"
import "testing"
import "time"
import "strings"

func TestOpenCompressedInfoDB(t *testing.T) {
	t.Parallel()
	flags := DefaultFlags()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)

	blockCount := 3
	created := addRecords(*flags.DIR, tableName, func(r *Record, index int) {
		r.AddIntField("id", int64(index), *flags.SKIP_OUTLIERS)
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age, *flags.SKIP_OUTLIERS)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
		r.AddIntField("time", int64(time.Now().Unix()), *flags.SKIP_OUTLIERS)
		r.AddStrField("name", fmt.Sprint("user", index))
	}, blockCount)

	nt := saveAndReloadTable(t, flags, tableName, blockCount)

	if nt.Name != tableName {
		t.Error("TEST TABLE NAME INCORRECT")
	}

	filename := fmt.Sprintf("db/%s/info.db", tableName)
	dat, err := ioutil.ReadFile(filename)

	if err != nil {
		fmt.Println("ERR", err)
		t.Error("Test table did not create info.db")
	}

	// NOW WE COMPRESS INFO.DB.GZ
	zfilename := fmt.Sprintf("db/%s/info.db.gz", tableName)
	file, err := os.Create(zfilename)
	if err != nil {
		t.Error("COULDNT LOAD ZIPPED TABLE FILE FOR WRITING!")

	}
	zinfo := gzip.NewWriter(file)
	zinfo.Write(dat)
	zinfo.Close()

	os.RemoveAll(filename)
	// END ZIPPING INFO.DB.GZ

	loadSpec := nt.NewLoadSpec()
	loadSpec.LoadAllColumns = true

	loaded := nt.LoadTableInfo()
	if !loaded {
		t.Error("COULDNT LOAD ZIPPED TABLE INFO!")
	}

	nt.LoadRecords(&loadSpec)

	var records = make([]*Record, 0)
	for _, b := range nt.BlockList {
		records = append(records, b.RecordList...)
	}

	if len(records) != len(created) {
		t.Error("More records were created than expected", len(records))
	}

}

func TestOpenCompressedColumn(t *testing.T) {
	t.Parallel()
	flags := DefaultFlags()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)

	blockCount := 3
	created := addRecords(*flags.DIR, tableName, func(r *Record, index int) {
		r.AddIntField("id", int64(index), *flags.SKIP_OUTLIERS)
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age, *flags.SKIP_OUTLIERS)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
		r.AddIntField("time", int64(time.Now().Unix()), *flags.SKIP_OUTLIERS)
		r.AddStrField("name", fmt.Sprint("user", index))
	}, blockCount)

	nt := saveAndReloadTable(t, flags, tableName, blockCount)
	nt.DigestRecords(flags)
	nt.LoadRecords(nil)

	blocks := nt.BlockList

	if nt.Name != tableName {
		t.Error("TEST TABLE NAME INCORRECT")
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
				t.Error("COULDNT LOAD ZIPPED TABLE FILE FOR WRITING!")

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

	bt := saveAndReloadTable(t, flags, tableName, blockCount)

	loadSpec := bt.NewLoadSpec()
	loadSpec.LoadAllColumns = true

	loaded := bt.LoadTableInfo()
	if !loaded {
		t.Error("COULDNT LOAD ZIPPED TABLE INFO!")
	}

	bt.LoadRecords(&loadSpec)

	var records = make([]*Record, 0)
	for _, b := range bt.BlockList {
		records = append(records, b.RecordList...)
	}

	if len(records) != len(created) {
		t.Error("More records were created than expected", len(records))
	}

}
