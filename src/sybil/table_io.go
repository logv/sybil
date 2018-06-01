package sybil

import "fmt"

import "os"
import "path"
import "sort"
import "strings"
import "sync"
import "time"
import "bytes"
import "io/ioutil"
import "encoding/gob"
import "runtime/debug"

var DEBUG_TIMING = false
var CHUNKS_BEFORE_GC = 16
var INGEST_DIR = "ingest"
var TEMP_INGEST_DIR = ".ingest.temp"
var CACHE_DIR = "cache"

var DELETE_BLOCKS_AFTER_QUERY = true
var HOLD_MATCHES = false
var BLOCKS_PER_CACHE_FILE = 64

func (t *Table) saveTableInfo(fname string) {
	if !t.GrabInfoLock() {
		return
	}

	defer t.ReleaseInfoLock()
	var network bytes.Buffer // Stand-in for the network.
	dirname := path.Join(*FLAGS.DIR, t.Name)
	filename := path.Join(dirname, fmt.Sprintf("%s.db", fname))
	backup := path.Join(dirname, fmt.Sprintf("%s.bak", fname))

	flagfile := path.Join(dirname, fmt.Sprintf("%s.db.exists", fname))

	// Create a backup file
	cp(backup, filename)

	// Create an encoder and send a value.
	enc := gob.NewEncoder(&network)
	err := enc.Encode(t)

	if err != nil {
		Error("encode:", err)
	}

	Debug("SERIALIZED TABLE INFO", fname, "INTO ", network.Len(), "BYTES")

	tempfile, err := ioutil.TempFile(dirname, "info.db")
	if err != nil {
		Error("ERROR CREATING TEMP FILE FOR TABLE INFO", err)
	}

	_, err = network.WriteTo(tempfile)
	if err != nil {
		Error("ERROR SAVING TABLE INFO INTO TEMPFILE", err)
	}

	RenameAndMod(tempfile.Name(), filename)
	os.Create(flagfile)
}

func (t *Table) SaveTableInfo(fname string) {
	saveTable := getSaveTable(t)
	saveTable.saveTableInfo(fname)

}

func getSaveTable(t *Table) *Table {
	return &Table{Name: t.Name,
		KeyTable: t.KeyTable,
		KeyTypes: t.KeyTypes,
		IntInfo:  t.IntInfo,
		StrInfo:  t.StrInfo}
}

func (t *Table) saveRecordList(records RecordList) bool {
	if len(records) == 0 {
		return false
	}

	Debug("SAVING RECORD LIST", len(records), t.Name)

	chunkSize := CHUNK_SIZE
	chunks := len(records) / chunkSize

	if chunks == 0 {
		filename, err := t.getNewIngestBlockName()
		if err != nil {
			Error("ERR SAVING BLOCK", filename, err)
		}
		t.SaveRecordsToBlock(records, filename)
	} else {
		for j := 0; j < chunks; j++ {
			filename, err := t.getNewIngestBlockName()
			if err != nil {
				Error("ERR SAVING BLOCK", filename, err)
			}
			t.SaveRecordsToBlock(records[j*chunkSize:(j+1)*chunkSize], filename)
		}

		// SAVE THE REMAINDER
		if len(records) > chunks*chunkSize {
			filename, err := t.getNewIngestBlockName()
			if err != nil {
				Error("Error creating new ingestion block", err)
			}

			t.SaveRecordsToBlock(records[chunks*chunkSize:], filename)
		}
	}

	return true
}

func (t *Table) SaveRecordsToColumns() bool {
	os.MkdirAll(path.Join(*FLAGS.DIR, t.Name), 0777)
	sort.Sort(SortRecordsByTime{t.newRecords})

	t.FillPartialBlock()
	ret := t.saveRecordList(t.newRecords)
	t.newRecords = make(RecordList, 0)
	t.SaveTableInfo("info")

	return ret

}

func (t *Table) LoadTableInfo() bool {
	tablename := t.Name
	filename := path.Join(*FLAGS.DIR, tablename, "info.db")
	if t.GrabInfoLock() {
		defer t.ReleaseInfoLock()
	} else {
		Debug("LOAD TABLE INFO LOCK TAKEN")
		return false
	}

	return t.LoadTableInfoFrom(filename)
}

func (t *Table) LoadTableInfoFrom(filename string) bool {
	savedTable := Table{Name: t.Name}
	savedTable.initDataStructures()

	start := time.Now()

	Debug("OPENING TABLE INFO FROM FILENAME", filename)
	err := decodeInto(filename, &savedTable)
	end := time.Now()
	if err != nil {
		Debug("TABLE INFO DECODE:", err)
		return false
	}

	if DEBUG_TIMING {
		Debug("TABLE INFO OPEN TOOK", end.Sub(start))
	}

	if len(savedTable.KeyTable) > 0 {
		t.KeyTable = savedTable.KeyTable
	}

	if len(savedTable.KeyTypes) > 0 {
		t.KeyTypes = savedTable.KeyTypes
	}

	if savedTable.IntInfo != nil {
		t.IntInfo = savedTable.IntInfo
	}
	if savedTable.StrInfo != nil {
		t.StrInfo = savedTable.StrInfo
	}

	// If we are recovering the INFO lock, we won't necessarily have
	// all fields filled out
	if t.stringIDMu != nil {
		t.populateStringIDLookup()
	}

	return true
}

// Remove our pointer to the blocklist so a GC is triggered and
// a bunch of new memory becomes available
func (t *Table) ReleaseRecords() {
	t.BlockList = make(map[string]*TableBlock)
	debug.FreeOSMemory()
}

func (t *Table) HasFlagFile() bool {
	// Make a determination of whether this is a new table or not. if it is a
	// new table, we are fine, but if it's not - we are in trouble!
	flagfile := path.Join(*FLAGS.DIR, t.Name, "info.db.exists")
	_, err := os.Open(flagfile)
	// If the flagfile exists and we couldn't read the file info, we are in trouble!
	if err == nil {
		t.ReleaseInfoLock()
		Warn("Table info missing, but flag file exists!")
		return true
	}

	return false

}

func fileLooksLikeBlock(v os.FileInfo) bool {

	switch {

	case v.Name() == INGEST_DIR || v.Name() == TEMP_INGEST_DIR:
		return false
	case v.Name() == CACHE_DIR:
		return false
	case strings.HasPrefix(v.Name(), STOMACHE_DIR):
		return false
	case strings.HasSuffix(v.Name(), "info.db"):
		return false
	case strings.HasSuffix(v.Name(), "old"):
		return false
	case strings.HasSuffix(v.Name(), "broken"):
		return false
	case strings.HasSuffix(v.Name(), "lock"):
		return false
	case strings.HasSuffix(v.Name(), "export"):
		return false
	case strings.HasSuffix(v.Name(), "partial"):
		return false
	}

	return true

}

func (t *Table) LoadBlockCache() {
	if !t.GrabCacheLock() {
		return
	}

	defer t.ReleaseCacheLock()
	files, err := ioutil.ReadDir(path.Join(*FLAGS.DIR, t.Name, CACHE_DIR))

	if err != nil {
		return
	}

	for _, blockFile := range files {
		filename := path.Join(*FLAGS.DIR, t.Name, CACHE_DIR, blockFile.Name())
		blockCache := SavedBlockCache{}
		if err != nil {
			continue
		}

		err = decodeInto(filename, &blockCache)
		if err != nil {
			continue
		}

		for k, v := range blockCache {
			t.BlockInfoCache[k] = v
		}
	}

	Debug("FILLED BLOCK CACHE WITH", len(t.BlockInfoCache), "ITEMS")
}

func (t *Table) ResetBlockCache() {
	t.BlockInfoCache = make(map[string]*SavedColumnInfo)
}

func (t *Table) WriteQueryCache(toCacheSpecs map[string]*QuerySpec) {

	// NOW WE SAVE OUR QUERY CACHE HERE...
	savestart := time.Now()
	var wg sync.WaitGroup

	saved := 0

	if *FLAGS.CACHED_QUERIES {
		for blockName, blockQuery := range toCacheSpecs {

			if blockName == INGEST_DIR || len(blockQuery.Results) > 5000 {
				continue
			}
			thisQuery := blockQuery
			thisName := blockName

			wg.Add(1)
			saved++
			go func() {

				thisQuery.SaveCachedResults(thisName)
				if *FLAGS.DEBUG {
					fmt.Fprint(os.Stderr, "s")
				}

				wg.Done()
			}()
		}

		wg.Wait()

		saveend := time.Now()

		if saved > 0 {
			if *FLAGS.DEBUG {
				fmt.Fprint(os.Stderr, "\n")
			}
			Debug("SAVING CACHED QUERIES TOOK", saveend.Sub(savestart))
		}
	}

	// END QUERY CACHE SAVING

}

func (t *Table) WriteBlockCache() {
	if len(t.NewBlockInfos) == 0 {
		return
	}

	if !t.GrabCacheLock() {
		return
	}

	defer t.ReleaseCacheLock()

	Debug("WRITING BLOCK CACHE, OUTSTANDING", len(t.NewBlockInfos))

	var numBlocks = len(t.NewBlockInfos) / BLOCKS_PER_CACHE_FILE

	for i := 0; i < numBlocks; i++ {
		cachedInfo := t.NewBlockInfos[i*BLOCKS_PER_CACHE_FILE : (i+1)*BLOCKS_PER_CACHE_FILE]

		blockFile, err := t.getNewCacheBlockFile()
		if err != nil {
			Debug("TROUBLE CREATING CACHE BLOCK FILE")
			break
		}
		blockCache := SavedBlockCache{}

		for _, blockName := range cachedInfo {
			blockCache[blockName] = t.BlockInfoCache[blockName]
		}

		enc := gob.NewEncoder(blockFile)
		err = enc.Encode(&blockCache)
		if err != nil {
			Debug("ERROR ENCODING BLOCK CACHE", err)
		}

		pathname := fmt.Sprintf("%s.db", blockFile.Name())

		Debug("RENAMING", blockFile.Name(), pathname)
		RenameAndMod(blockFile.Name(), pathname)

	}

	t.NewBlockInfos = t.NewBlockInfos[:0]

}

func (t *Table) LoadRecords(loadSpec *LoadSpec) int {
	t.LoadBlockCache()

	return t.LoadAndQueryRecords(loadSpec, nil)
}

func (t *Table) ChunkAndSave() {

	if len(t.newRecords) >= CHUNK_SIZE {
		os.MkdirAll(path.Join(*FLAGS.DIR, t.Name), 0777)
		name, err := t.getNewIngestBlockName()
		if err == nil {
			t.SaveRecordsToBlock(t.newRecords, name)
			t.SaveTableInfo("info")
			t.newRecords = make(RecordList, 0)
			t.ReleaseRecords()
		} else {
			Error("ERROR SAVING BLOCK", err)
		}
	}

}

func (t *Table) IsNotExist() bool {
	tableDir := path.Join(*FLAGS.DIR, t.Name)
	_, err := ioutil.ReadDir(tableDir)
	return err != nil
}
