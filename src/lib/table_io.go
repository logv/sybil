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

var DebugTiming = false
var ChunksBeforeGC = 16
var IngestDir = "ingest"
var TempIngestDir = ".ingest.temp"
var CacheDir = "cache"

var DeleteBlocksAfterQuery = true
var HoldMatches = false
var BlocksPerCacheFile = 64

// TODO: We should really split this into two functions based on dir / file
func RenameAndMod(src, dst string) error {
	os.Chmod(src, 0755)
	return os.Rename(src, dst)
}

func (t *Table) saveTableInfo(fname string) {
	if t.GrabInfoLock() == false {
		return
	}

	defer t.ReleaseInfoLock()
	var network bytes.Buffer // Stand-in for the network.
	dirname := path.Join(*FLAGS.Dir, t.Name)
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

	chunkSize := ChunkSize
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
	os.MkdirAll(path.Join(*FLAGS.Dir, t.Name), 0777)
	sort.Sort(SortRecordsByTime{t.newRecords})

	t.FillPartialBlock()
	ret := t.saveRecordList(t.newRecords)
	t.newRecords = make(RecordList, 0)
	t.SaveTableInfo("info")

	return ret

}

func (t *Table) LoadTableInfo() bool {
	tablename := t.Name
	filename := path.Join(*FLAGS.Dir, tablename, "info.db")
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

	if DebugTiming {
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
	if t.stringIDMutex != nil {
		t.populateStringIDLookup()
	}

	return true
}

// Remove our pointer to the blocklist so a GC is triggered and
// a bunch of new memory becomes available
func (t *Table) ReleaseRecords() {
	t.BlockList = make(map[string]*TableBlock, 0)
	debug.FreeOSMemory()
}

func (t *Table) HasFlagFile() bool {
	// Make a determination of whether this is a new table or not. if it is a
	// new table, we are fine, but if it's not - we are in trouble!
	flagfile := path.Join(*FLAGS.Dir, t.Name, "info.db.exists")
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

	case v.Name() == IngestDir || v.Name() == TempIngestDir:
		return false
	case v.Name() == CacheDir:
		return false
	case strings.HasPrefix(v.Name(), StomacheDir):
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
	if t.GrabCacheLock() == false {
		return
	}

	defer t.ReleaseCacheLock()
	files, err := ioutil.ReadDir(path.Join(*FLAGS.Dir, t.Name, CacheDir))

	if err != nil {
		return
	}

	for _, blockFile := range files {
		filename := path.Join(*FLAGS.Dir, t.Name, CacheDir, blockFile.Name())
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
	t.BlockInfoCache = make(map[string]*SavedColumnInfo, 0)
}

func (t *Table) WriteQueryCache(toCacheSpecs map[string]*QuerySpec) {

	// NOW WE SAVE OUR QUERY CACHE HERE...
	savestart := time.Now()

	if *FLAGS.CachedQueries {
		for blockName, blockQuery := range toCacheSpecs {

			if blockName == IngestDir {
				continue
			}

			blockQuery.SaveCachedResults(blockName)
			if *FLAGS.Debug {
				fmt.Fprint(os.Stderr, "s")
			}
		}

		saveend := time.Now()

		if len(toCacheSpecs) > 0 {
			if *FLAGS.Debug {
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

	if t.GrabCacheLock() == false {
		return
	}

	defer t.ReleaseCacheLock()

	Debug("WRITING BLOCK CACHE, OUTSTANDING", len(t.NewBlockInfos))

	var numBlocks = len(t.NewBlockInfos) / BlocksPerCacheFile

	for i := 0; i < numBlocks; i++ {
		cachedInfo := t.NewBlockInfos[i*BlocksPerCacheFile : (i+1)*BlocksPerCacheFile]

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

func (t *Table) LoadAndQueryRecords(loadSpec *LoadSpec, querySpec *QuerySpec) int {
	waystart := time.Now()
	Debug("LOADING", *FLAGS.Dir, t.Name)

	files, _ := ioutil.ReadDir(path.Join(*FLAGS.Dir, t.Name))

	if ReadRowsOnly {
		Debug("ONLY READING RECORDS FROM ROW STORE")
		files = nil
	}

	if querySpec != nil {

		querySpec.Table = t
	}

	// Load and setup our OPTS.StrReplacements
	OPTS.StrReplacements = make(map[string]StrReplace)
	if FLAGS.StrReplace != nil {
		var replacements = strings.Split(*FLAGS.StrReplace, *FLAGS.FieldSeparator)
		for _, repl := range replacements {
			tokens := strings.Split(repl, ":")
			if len(tokens) > 2 {
				col := tokens[0]
				pattern := tokens[1]
				replacement := tokens[2]
				OPTS.StrReplacements[col] = StrReplace{pattern, replacement}
			}
		}
	}

	var wg sync.WaitGroup
	blockSpecs := make(map[string]*QuerySpec)
	toCacheSpecs := make(map[string]*QuerySpec)

	loadedInfo := t.LoadTableInfo()
	if loadedInfo == false {
		if t.HasFlagFile() {
			return 0
		}
	}

	if *FLAGS.UpdateTableInfo {
		Debug("RESETTING TABLE INFO FOR OVERWRITING")
		t.IntInfo = make(IntInfoTable)
		t.StrInfo = make(StrInfoTable)
	}

	m := &sync.Mutex{}

	loadAll := false
	if loadSpec != nil && loadSpec.LoadAllColumns {
		loadAll = true
	}

	count := 0
	cachedCount := 0
	cachedBlocks := 0
	loadedCount := 0
	skipped := 0
	brokenCount := 0
	thisBlock := 0
	blockGcTime := time.Now().Sub(time.Now())

	brokenMutex := sync.Mutex{}
	brokenBlocks := make([]string, 0)
	for f := range files {

		// TODO: decide more formally on order of block loading
		// SAMPLES: reverse chronological order
		// EVERYTHING ELSE: chronological order
		v := files[f]
		if OPTS.Samples {
			v = files[len(files)-f-1]
		}

		if v.IsDir() && fileLooksLikeBlock(v) {
			filename := path.Join(*FLAGS.Dir, t.Name, v.Name())
			thisBlock++

			wg.Add(1)
			go func() {
				defer wg.Done()

				start := time.Now()

				shouldLoad := t.ShouldLoadBlockFromDir(filename, querySpec)

				if !shouldLoad {
					skipped++
					return
				}

				var cachedSpec *QuerySpec
				var cachedBlock *TableBlock

				if querySpec != nil {
					cachedBlock, cachedSpec = t.getCachedQueryForBlock(filename, querySpec)
				}

				var block *TableBlock
				if cachedSpec == nil {
					// couldnt load the cached query results
					block = t.LoadBlockFromDir(filename, loadSpec, loadAll)
					if block == nil {
						brokenMutex.Lock()
						brokenBlocks = append(brokenBlocks, filename)
						brokenMutex.Unlock()
						return
					}
				} else {
					// we are using cached query results
					block = cachedBlock
				}

				if *FLAGS.Debug {
					if cachedSpec != nil {
						fmt.Fprint(os.Stderr, "c")
					} else {
						fmt.Fprint(os.Stderr, ".")

					}
				}

				end := time.Now()
				if DebugTiming {
					if loadSpec != nil {
						Debug("LOADED BLOCK FROM DIR", filename, "TOOK", end.Sub(start))
					} else {
						Debug("LOADED INFO FOR BLOCK", filename, "TOOK", end.Sub(start))
					}
				}

				if len(block.RecordList) > 0 || cachedSpec != nil {
					if querySpec == nil {
						m.Lock()
						count += len(block.RecordList)
						m.Unlock()
					} else { // Load and Query
						blockQuery := cachedSpec
						if blockQuery == nil {
							blockQuery = CopyQuerySpec(querySpec)
							blockQuery.MatchedCount = FilterAndAggRecords(blockQuery, &block.RecordList)

							if HoldMatches {
								block.Matched = blockQuery.Matched
							}

						}

						if blockQuery != nil {
							m.Lock()
							if cachedSpec != nil {
								cachedCount += blockQuery.MatchedCount
								cachedBlocks += 1

							} else {
								count += blockQuery.MatchedCount
								loadedCount += 1
								if block.Info.NumRecords == int32(ChunkSize) {
									toCacheSpecs[block.Name] = blockQuery
								}
							}
							blockSpecs[block.Name] = blockQuery
							m.Unlock()
						}
					}

				}

				if OPTS.WriteBlockInfo {
					block.SaveInfoToColumns(block.Name)
				}

				if *FLAGS.Export {
					block.ExportBlockData()
				}
				// don't delete when testing so we can verify block
				// loading results
				if loadSpec != nil && DeleteBlocksAfterQuery && TestMode == false {
					t.blockMutex.Lock()
					tb, ok := t.BlockList[block.Name]
					if ok {
						tb.RecycleSlab(loadSpec)

						delete(t.BlockList, block.Name)
					}
					t.blockMutex.Unlock()

				}
			}()

			if *FLAGS.Samples {
				wg.Wait()

				if count > *FLAGS.Limit {
					break
				}
			}

			if DeleteBlocksAfterQuery && thisBlock%ChunksBeforeGC == 0 && *FLAGS.GC {
				wg.Wait()
				start := time.Now()

				if *FLAGS.RecycleMem == false {
					m.Lock()
					oldPercent := debug.SetGCPercent(100)
					debug.SetGCPercent(oldPercent)
					m.Unlock()
				}

				if *FLAGS.Debug {
					fmt.Fprint(os.Stderr, ",")
				}
				end := time.Now()
				blockGcTime += end.Sub(start)
			}
		}

	}

	rowStoreQuery := AfterLoadQueryCB{}
	var logend time.Time
	logstart := time.Now()
	if *FLAGS.ReadIngestionLog {
		if querySpec == nil {
			rowStoreQuery.querySpec = &QuerySpec{}
			rowStoreQuery.querySpec.Table = t
			rowStoreQuery.querySpec.Punctuate()
		} else {
			rowStoreQuery.querySpec = CopyQuerySpec(querySpec)
			rowStoreQuery.querySpec.Table = t
		}

		// Entrust AfterLoadQueryCB to call Done on wg
		rowStoreQuery.wg = &wg
		m.Lock()
		blockSpecs[IngestDir] = rowStoreQuery.querySpec
		m.Unlock()

		wg.Add(1)
		go func() {
			t.LoadRowStoreRecords(IngestDir, rowStoreQuery.CB)
			m.Lock()
			logend = time.Now()
			m.Unlock()
		}()
	}

	wg.Wait()

	if *FLAGS.Debug {
		fmt.Fprint(os.Stderr, "\n")
	}

	for _, brokenBlockName := range brokenBlocks {
		Debug("BLOCK", brokenBlockName, "IS BROKEN, SKIPPING")
	}

	if *FLAGS.ReadIngestionLog {
		m.Lock()
		Debug("LOADING & QUERYING INGESTION LOG TOOK", logend.Sub(logstart))
		Debug("INGESTION LOG RECORDS MATCHED", rowStoreQuery.count)
		m.Unlock()
		count += rowStoreQuery.count

		if DeleteBlocksAfterQuery == false && t.RowBlock != nil {
			Debug("ROW STORE RECORD LENGTH IS", len(rowStoreQuery.records))
			t.RowBlock.RecordList = rowStoreQuery.records
			t.RowBlock.Matched = rowStoreQuery.records
		}
	}

	if blockGcTime > 0 {
		Debug("BLOCK GC TOOK", blockGcTime)
	}

	// RE-POPULATE LOOKUP TABLE INFO
	t.populateStringIDLookup()

	Debug("SKIPPED", skipped, "BLOCKS BASED ON PRE FILTERS")
	Debug("SKIPPED", brokenCount, "BLOCKS BASED ON BROKEN INFO")
	Debug("SKIPPED", cachedBlocks, "BLOCKS &", cachedCount, "RECORDS BASED ON QUERY CACHE")
	end := time.Now()
	if loadSpec != nil {
		Debug("LOADED", count, "RECORDS FROM", loadedCount, "BLOCKS INTO", t.Name, "TOOK", end.Sub(waystart))
	} else {
		Debug("INSPECTED", len(t.BlockList), "BLOCKS", "TOOK", end.Sub(waystart))
	}

	// NOTE: we have to write the query cache before we combine our results,
	// bc combining results is not idempotent
	t.WriteQueryCache(toCacheSpecs)

	if FLAGS.LoadAndQuery != nil && *FLAGS.LoadAndQuery == true && querySpec != nil {
		// COMBINE THE PER BLOCK RESULTS
		astart := time.Now()
		resultSpec := CombineResults(querySpec, blockSpecs)

		aend := time.Now()
		Debug("AGGREGATING RESULT BLOCKS TOOK", aend.Sub(astart))

		querySpec.Cumulative = resultSpec.Cumulative

		querySpec.Results = resultSpec.Results
		querySpec.TimeResults = resultSpec.TimeResults

		SortResults(querySpec)
	}

	t.WriteBlockCache()

	return count

}

func (t *Table) LoadRecords(loadSpec *LoadSpec) int {
	t.LoadBlockCache()

	return t.LoadAndQueryRecords(loadSpec, nil)
}

func (t *Table) ChunkAndSave() {

	if len(t.newRecords) >= ChunkSize {
		os.MkdirAll(path.Join(*FLAGS.Dir, t.Name), 0777)
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
	tableDir := path.Join(*FLAGS.Dir, t.Name)
	_, err := ioutil.ReadDir(tableDir)
	return err != nil
}
