package sybil

import "time"
import "path"
import "io/ioutil"

import "os"
import "strings"

// to ingest, make a new tmp file inside ingest/ (or append to an existing one)
// to digest, make a new STOMACHE_DIR tempdir and move all files from ingest/ into it

func (t *Table) getNewIngestBlockName() (string, error) {
	Debug("GETTING INGEST BLOCK NAME", t.Dir, "TABLE", t.Name)
	name, err := ioutil.TempDir(path.Join(t.Dir, t.Name), "block")
	return name, err
}

func (t *Table) getNewCacheBlockFile() (*os.File, error) {
	Debug("GETTING CACHE BLOCK NAME", t.Dir, "TABLE", t.Name)
	tableCacheDir := path.Join(t.Dir, t.Name, CACHE_DIR)
	os.MkdirAll(tableCacheDir, 0755)

	// this info block needs to be moved once its finished being written to
	file, err := ioutil.TempFile(tableCacheDir, "info")
	return file, err
}

// Go through newRecords list and save all the new records out to a row store
func (t *Table) IngestRecords(skipCompact bool, blockname string) (minFilesToDigest int) {
	Debug("KEY TABLE", t.KeyTable)
	Debug("KEY TYPES", t.KeyTypes)

	t.AppendRecordsToLog(t.newRecords[:], blockname)
	t.newRecords = make(RecordList, 0)
	t.SaveTableInfo("info")
	t.ReleaseRecords()

	if !skipCompact {
		_, minFilesToDigest = t.MaybeCompactRecords()
	}
	return minFilesToDigest
}

// TODO: figure out how often we actually do a collation check by storing last
// collation inside a file somewhere
func (t *Table) CompactRecords(minFilesToDigest int) {
	HOLD_MATCHES = true

	t.ResetBlockCache()
	t.DigestRecords(minFilesToDigest)

}

// we compact if:
// we have over X files
// we have over X megabytes of data
// remember, there is no reason to actually read the data off disk
// until we decide to compact
func (t *Table) MaybeCompactRecords() (compacted bool, minFilesToDigest int) {
	should, minFilesToDigest := t.ShouldCompactRowStore(INGEST_DIR)
	if should {
		t.CompactRecords(minFilesToDigest)
	}
	return should, minFilesToDigest
}

var NO_MORE_BLOCKS = GROUP_DELIMITER

type AfterRowBlockLoad func(dir string, tableName string, params HistogramParameters, delim string, rl RecordList)

var FILE_DIGEST_THRESHOLD = 256
var KB = int64(1024)
var SIZE_DIGEST_THRESHOLD = int64(1024) * 2
var MAX_ROW_STORE_TRIES = 20

func (t *Table) ShouldCompactRowStore(digest string) (should bool, minFilesToDigest int) {
	dirname := path.Join(t.Dir, t.Name, digest)
	// if the row store dir does not exist, skip the whole function
	_, err := os.Stat(dirname)
	if os.IsNotExist(err) {
		return false, 0
	}

	var file *os.File
	for i := 0; i < LOCK_TRIES; i++ {
		file, err = os.Open(dirname)
		if err != nil {
			Debug("Can't open the ingestion dir", dirname)
			time.Sleep(LOCK_US)
			if i > MAX_ROW_STORE_TRIES {
				return false, 0
			}

			continue
		}
		break
	}

	files, _ := file.Readdir(0)
	minFilesToDigest = len(files)

	if len(files) > FILE_DIGEST_THRESHOLD {
		return true, minFilesToDigest
	}

	size := int64(0)
	for _, f := range files {
		size = size + f.Size()
	}

	// compact every MB or so
	if size/KB > SIZE_DIGEST_THRESHOLD {
		return true, minFilesToDigest
	}

	return false, minFilesToDigest
}

func (t *Table) LoadRowStoreRecords(digest string, loadSpec *LoadSpec, afterBlockLoadCb AfterRowBlockLoad) {
	dirname := path.Join(t.Dir, t.Name, digest)
	var err error

	// if the row store dir does not exist, skip the whole function
	_, err = os.Stat(dirname)
	if os.IsNotExist(err) {
		if afterBlockLoadCb != nil {
			afterBlockLoadCb(t.Dir, t.Name, HistogramParameters{}, NO_MORE_BLOCKS, nil)
		}

		return
	}

	var file *os.File
	for i := 0; i < LOCK_TRIES; i++ {
		file, err = os.Open(dirname)
		if err != nil {
			Debug("Can't open the ingestion dir", dirname)
			time.Sleep(LOCK_US)
			if i > MAX_ROW_STORE_TRIES {
				return
			}
			continue
		}
		break
	}

	files, _ := file.Readdir(0)
	if t.RowBlock == nil {
		t.RowBlock = &TableBlock{}
		(*t.RowBlock).RecordList = make(RecordList, 0)
		t.RowBlock.Info = &SavedColumnInfo{}
		t.blockMu.Lock()
		t.BlockList[ROW_STORE_BLOCK] = t.RowBlock
		t.blockMu.Unlock()
		t.RowBlock.Name = ROW_STORE_BLOCK
	}

	for _, file := range files {
		filename := file.Name()

		// we can open .gz files as well as regular .db files
		cname := strings.TrimRight(filename, GZIP_EXT)

		if !strings.HasSuffix(cname, ".db") {
			continue
		}

		filename = path.Join(dirname, file.Name())

		records := t.LoadRecordsFromLog(filename, loadSpec)
		if afterBlockLoadCb != nil {
			afterBlockLoadCb(t.Dir, t.Name, HistogramParameters{}, filename, records)
		}
	}

	if afterBlockLoadCb != nil {
		afterBlockLoadCb(t.Dir, t.Name, HistogramParameters{}, NO_MORE_BLOCKS, nil)
	}

}

func LoadRowBlockCB(dir string, tableName string, digestname string, records RecordList) {
	if digestname == NO_MORE_BLOCKS {
		return
	}

	t := GetTable(dir, tableName)
	block := t.RowBlock

	if len(records) > 0 {
		block.RecordList = append(block.RecordList, records...)
		block.Info.NumRecords = int32(len(block.RecordList))
	}

}

func (t *Table) RestoreUningestedFiles() {
	if !t.GrabDigestLock() {
		Debug("CANT RESTORE UNINGESTED RECORDS WITHOUT DIGEST LOCK")
		return
	}

	ingestdir := path.Join(t.Dir, t.Name, INGEST_DIR)
	os.MkdirAll(ingestdir, 0777)

	digesting := path.Join(t.Dir, t.Name)
	file, _ := os.Open(digesting)
	dirs, _ := file.Readdir(0)

	for _, dir := range dirs {
		if strings.HasPrefix(dir.Name(), STOMACHE_DIR) && dir.IsDir() {
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
	digestDir    string
	deleteBlocks []string
}

func (cb *SaveBlockChunkCB) CB(dir string, tableName string, params HistogramParameters, digestname string, records RecordList) {

	t := GetTable(dir, tableName)
	if digestname == NO_MORE_BLOCKS {
		if len(t.newRecords) > 0 {
			t.SaveRecordsToColumns()
			t.ReleaseRecords()
		}

		for _, file := range cb.deleteBlocks {
			Debug("REMOVING", file)
			os.Remove(file)
		}

		dir, err := os.Open(cb.digestDir)
		if err == nil {
			contents, err := dir.Readdir(0)

			if err == nil && len(contents) == 0 {
				os.RemoveAll(cb.digestDir)
			}
		}
		t.ReleaseDigestLock()
		return
	}

	Debug("LOADED", len(records), "FOR DIGESTION FROM", digestname)
	if len(records) > 0 {
		t.newRecords = append(t.newRecords, records...)
	}
	cb.deleteBlocks = append(cb.deleteBlocks, digestname)

}

var STOMACHE_DIR = "stomache"

// Go through rowstore and save records out to column store
func (t *Table) DigestRecords(minFilesToDigest int) {
	canDigest := t.GrabDigestLock()

	if !canDigest {
		t.ReleaseInfoLock()
		Debug("CANT GRAB LOCK FOR DIGEST RECORDS")
		return
	}

	dirname := path.Join(t.Dir, t.Name)
	digestfile := path.Join(dirname, INGEST_DIR)
	digesting, err := ioutil.TempDir(dirname, STOMACHE_DIR)

	// TODO: we need to figure a way out such that the STOMACHE_DIR isn't going
	// to ruin us if it still exists (bc some proc didn't clean up after itself)
	if err != nil {
		t.ReleaseDigestLock()
		Debug("ERROR CREATING DIGESTION DIR", err)
		time.Sleep(time.Millisecond * 50)
		return
	}

	file, _ := os.Open(digestfile)

	files, err := file.Readdir(0)
	if len(files) < minFilesToDigest {
		Debug("SKIPPING DIGESTION, NOT AS MANY FILES AS WE THOUGHT", len(files), "VS", minFilesToDigest)
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
		cb := SaveBlockChunkCB{digestDir: digesting}
		t.LoadRowStoreRecords(basename, nil, cb.CB)
	} else {
		t.ReleaseDigestLock()
	}
}
