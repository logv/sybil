package sybil

import "time"
import "path"
import "io/ioutil"

import "os"
import "strings"

// to ingest, make a new tmp file inside ingest/ (or append to an existing one)
// to digest, make a new StomacheDir tempdir and move all files from ingest/ into it

var ReadRowsOnly = false
var MinFilesToDigest = 0

func (t *Table) getNewIngestBlockName() (string, error) {
	Debug("GETTING INGEST BLOCK NAME", *FLAGS.Dir, "TABLE", t.Name)
	name, err := ioutil.TempDir(path.Join(*FLAGS.Dir, t.Name), "block")
	return name, err
}

func (t *Table) getNewCacheBlockFile() (*os.File, error) {
	Debug("GETTING CACHE BLOCK NAME", *FLAGS.Dir, "TABLE", t.Name)
	tableCacheDir := path.Join(*FLAGS.Dir, t.Name, CacheDir)
	os.MkdirAll(tableCacheDir, 0755)

	// this info block needs to be moved once its finished being written to
	file, err := ioutil.TempFile(tableCacheDir, "info")
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

	t.MaybeCompactRecords()
}

// TODO: figure out how often we actually do a collation check by storing last
// collation inside a file somewhere
func (t *Table) CompactRecords() {
	FLAGS.ReadIngestionLog = &trueFlag
	ReadRowsOnly = true
	DeleteBlocksAfterQuery = false
	HoldMatches = true

	t.ResetBlockCache()
	t.DigestRecords()

}

// we compact if:
// we have over X files
// we have over X megabytes of data
// remember, there is no reason to actually read the data off disk
// until we decide to compact
func (t *Table) MaybeCompactRecords() {
	if *FLAGS.SkipCompact == true {
		return
	}

	if t.ShouldCompactRowStore(IngestDir) {
		t.CompactRecords()
	}
}

var NoMoreBlocks = GroupDelimiter

type AfterRowBlockLoad func(string, RecordList)

var FileDigestThreshold = 256
var KB = int64(1024)
var SizeDigestThreshold = int64(1024) * 2
var MaxRowStoreTries = 20

func (t *Table) ShouldCompactRowStore(digest string) bool {
	dirname := path.Join(*FLAGS.Dir, t.Name, digest)
	// if the row store dir does not exist, skip the whole function
	_, err := os.Stat(dirname)
	if os.IsNotExist(err) {
		return false
	}

	var file *os.File
	for i := 0; i < LockTries; i++ {
		file, err = os.Open(dirname)
		if err != nil {
			Debug("Can't open the ingestion dir", dirname)
			time.Sleep(LockUs)
			if i > MaxRowStoreTries {
				return false
			}

			continue
		}
		break
	}

	files, err := file.Readdir(0)
	MinFilesToDigest = len(files)

	if len(files) > FileDigestThreshold {
		return true
	}

	size := int64(0)
	for _, f := range files {
		size = size + f.Size()
	}

	// compact every MB or so
	if size/KB > SizeDigestThreshold {
		return true
	}

	return false

}
func (t *Table) LoadRowStoreRecords(digest string, afterBlockLoadCb AfterRowBlockLoad) {
	dirname := path.Join(*FLAGS.Dir, t.Name, digest)
	var err error

	// if the row store dir does not exist, skip the whole function
	_, err = os.Stat(dirname)
	if os.IsNotExist(err) {
		if afterBlockLoadCb != nil {
			afterBlockLoadCb(NoMoreBlocks, nil)
		}

		return
	}

	var file *os.File
	for i := 0; i < LockTries; i++ {
		file, err = os.Open(dirname)
		if err != nil {
			Debug("Can't open the ingestion dir", dirname)
			time.Sleep(LockUs)
			if i > MaxRowStoreTries {
				return
			}
			continue
		}
		break
	}

	files, err := file.Readdir(0)
	if t.RowBlock == nil {
		t.RowBlock = &TableBlock{}
		(*t.RowBlock).RecordList = make(RecordList, 0)
		t.RowBlock.Info = &SavedColumnInfo{}
		t.blockMutex.Lock()
		t.BlockList[RowStoreBlock] = t.RowBlock
		t.blockMutex.Unlock()
		t.RowBlock.Name = RowStoreBlock
	}

	for _, file := range files {
		filename := file.Name()

		// we can open .gz files as well as regular .db files
		cname := strings.TrimRight(filename, GzipExt)

		if strings.HasSuffix(cname, ".db") == false {
			continue
		}

		filename = path.Join(dirname, file.Name())

		records := t.LoadRecordsFromLog(filename)
		if afterBlockLoadCb != nil {
			afterBlockLoadCb(filename, records)
		}
	}

	if afterBlockLoadCb != nil {
		afterBlockLoadCb(NoMoreBlocks, nil)
	}

}

func LoadRowBlockCB(digestname string, records RecordList) {
	if digestname == NoMoreBlocks {
		return
	}

	t := GetTable(*FLAGS.Table)
	block := t.RowBlock

	if len(records) > 0 {
		block.RecordList = append(block.RecordList, records...)
		block.Info.NumRecords = int32(len(block.RecordList))
	}

}

var DeleteBlocks = make([]string, 0)

func (t *Table) RestoreUningestedFiles() {
	if t.GrabDigestLock() == false {
		Debug("CANT RESTORE UNINGESTED RECORDS WITHOUT DIGEST LOCK")
		return
	}

	ingestdir := path.Join(*FLAGS.Dir, t.Name, IngestDir)
	os.MkdirAll(ingestdir, 0777)

	digesting := path.Join(*FLAGS.Dir, t.Name)
	file, _ := os.Open(digesting)
	dirs, _ := file.Readdir(0)

	for _, dir := range dirs {
		if strings.HasPrefix(dir.Name(), StomacheDir) && dir.IsDir() {
			fname := path.Join(digesting, dir.Name())
			file, _ := os.Open(fname)
			files, _ := file.Readdir(0)
			for _, file := range files {
				Debug("RESTORING UNINGESTED FILE", file.Name())
				from := path.Join(fname, file.Name())
				to := path.Join(ingestdir, file.Name())
				err := RenameAndMod(from, to)
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

	t := GetTable(*FLAGS.Table)
	if digestname == NoMoreBlocks {
		if len(t.newRecords) > 0 {
			t.SaveRecordsToColumns()
			t.ReleaseRecords()
		}

		for _, file := range DeleteBlocks {
			Debug("REMOVING", file)
			os.Remove(file)
		}

		dir, err := os.Open(cb.digestdir)
		if err == nil {
			contents, err := dir.Readdir(0)

			if err == nil && len(contents) == 0 {
				os.RemoveAll(cb.digestdir)
			}
		}
		t.ReleaseDigestLock()
		return
	}

	Debug("LOADED", len(records), "FOR DIGESTION FROM", digestname)
	if len(records) > 0 {
		t.newRecords = append(t.newRecords, records...)
	}
	DeleteBlocks = append(DeleteBlocks, digestname)

}

var StomacheDir = "stomache"

// Go through rowstore and save records out to column store
func (t *Table) DigestRecords() {
	canDigest := t.GrabDigestLock()

	if !canDigest {
		t.ReleaseInfoLock()
		Debug("CANT GRAB LOCK FOR DIGEST RECORDS")
		return
	}

	dirname := path.Join(*FLAGS.Dir, t.Name)
	digestfile := path.Join(dirname, IngestDir)
	digesting, err := ioutil.TempDir(dirname, StomacheDir)

	// TODO: we need to figure a way out such that the StomacheDir isn't going
	// to ruin us if it still exists (bc some proc didn't clean up after itself)
	if err != nil {
		t.ReleaseDigestLock()
		Debug("ERROR CREATING DIGESTION DIR", err)
		time.Sleep(time.Millisecond * 50)
		return
	}

	file, _ := os.Open(digestfile)

	files, err := file.Readdir(0)
	if len(files) < MinFilesToDigest {
		Debug("SKIPPING DIGESTION, NOT AS MANY FILES AS WE THOUGHT", len(files), "VS", MinFilesToDigest)
		t.ReleaseDigestLock()
		return
	}

	if err == nil {
		for _, f := range files {
			RenameAndMod(path.Join(digestfile, f.Name()), path.Join(digesting, f.Name()))
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
