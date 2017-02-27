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
	if t.GrabInfoLock() == false {
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

	os.Rename(tempfile.Name(), filename)
	os.Create(flagfile)
}

func (t *Table) SaveTableInfo(fname string) {
	save_table := getSaveTable(t)
	save_table.saveTableInfo(fname)

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

	chunk_size := CHUNK_SIZE
	chunks := len(records) / chunk_size

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
			t.SaveRecordsToBlock(records[j*chunk_size:(j+1)*chunk_size], filename)
		}

		// SAVE THE REMAINDER
		if len(records) > chunks*chunk_size {
			filename, err := t.getNewIngestBlockName()
			if err != nil {
				Error("Error creating new ingestion block", err)
			}

			t.SaveRecordsToBlock(records[chunks*chunk_size:], filename)
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
	saved_table := Table{Name: t.Name}
	saved_table.init_data_structures()

	start := time.Now()

	Debug("OPENING TABLE INFO FROM FILENAME", filename)
	dec := GetFileDecoder(filename)
	err := dec.Decode(&saved_table)
	end := time.Now()
	if err != nil {
		Debug("TABLE INFO DECODE:", err)
		return false
	}

	if DEBUG_TIMING {
		Debug("TABLE INFO OPEN TOOK", end.Sub(start))
	}

	if len(saved_table.KeyTable) > 0 {
		t.KeyTable = saved_table.KeyTable
	}

	if len(saved_table.KeyTypes) > 0 {
		t.KeyTypes = saved_table.KeyTypes
	}

	if saved_table.IntInfo != nil {
		t.IntInfo = saved_table.IntInfo
	}
	if saved_table.StrInfo != nil {
		t.StrInfo = saved_table.StrInfo
	}

	// If we are recovering the INFO lock, we won't necessarily have
	// all fields filled out
	if t.string_id_m != nil {
		t.populate_string_id_lookup()
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

func file_looks_like_block(v os.FileInfo) bool {

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
	files, err := ioutil.ReadDir(path.Join(*FLAGS.DIR, t.Name, CACHE_DIR))

	if err != nil {
		return
	}

	for _, block_file := range files {
		filename := path.Join(*FLAGS.DIR, t.Name, CACHE_DIR, block_file.Name())
		block_cache := SavedBlockCache{}
		if err != nil {
			continue
		}

		dec := GetFileDecoder(filename)
		err = dec.Decode(&block_cache)
		if err != nil {
			continue
		}

		for k, v := range block_cache {
			t.BlockInfoCache[k] = v
		}
	}

	Debug("FILLED BLOCK CACHE WITH", len(t.BlockInfoCache), "ITEMS")
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

	var num_blocks = len(t.NewBlockInfos) / BLOCKS_PER_CACHE_FILE

	for i := 0; i < num_blocks; i++ {
		cached_info := t.NewBlockInfos[i*BLOCKS_PER_CACHE_FILE : (i+1)*BLOCKS_PER_CACHE_FILE]

		block_file, err := t.getNewCacheBlockFile()
		if err != nil {
			Debug("TROUBLE CREATING CACHE BLOCK FILE")
			break
		}
		block_cache := SavedBlockCache{}

		for _, block_name := range cached_info {
			block_cache[block_name] = t.BlockInfoCache[block_name]
		}

		enc := gob.NewEncoder(block_file)
		err = enc.Encode(&block_cache)
		if err != nil {
			Debug("ERROR ENCODING BLOCK CACHE", err)
		}

		pathname := fmt.Sprintf("%s.db", block_file.Name())

		Debug("RENAMING", block_file.Name(), pathname)
		os.Rename(block_file.Name(), pathname)

	}

	t.NewBlockInfos = t.NewBlockInfos[:0]

}

func (t *Table) LoadAndQueryRecords(loadSpec *LoadSpec, querySpec *QuerySpec) int {
	waystart := time.Now()
	Debug("LOADING", *FLAGS.DIR, t.Name)

	files, _ := ioutil.ReadDir(path.Join(*FLAGS.DIR, t.Name))

	if READ_ROWS_ONLY {
		Debug("ONLY READING RECORDS FROM ROW STORE")
		files = nil
	}

	if querySpec != nil {

		querySpec.Table = t
	}

	// Load and setup our OPTS.STR_REPLACEMENTS
	OPTS.STR_REPLACEMENTS = make(map[string]StrReplace)
	if FLAGS.STR_REPLACE != nil {
		var replacements = strings.Split(*FLAGS.STR_REPLACE, ",")
		for _, repl := range replacements {
			tokens := strings.Split(repl, ":")
			if len(tokens) > 2 {
				col := tokens[0]
				pattern := tokens[1]
				replacement := tokens[2]
				OPTS.STR_REPLACEMENTS[col] = StrReplace{pattern, replacement}
			}
		}
	}

	var wg sync.WaitGroup
	block_specs := make(map[string]*QuerySpec)

	loaded_info := t.LoadTableInfo()
	if loaded_info == false {
		if t.HasFlagFile() {
			return 0
		}
	}

	if *FLAGS.UPDATE_TABLE_INFO {
		Debug("RESETTING TABLE INFO FOR OVERWRITING")
		t.IntInfo = make(IntInfoTable)
		t.StrInfo = make(StrInfoTable)
	}

	m := &sync.Mutex{}

	load_all := false
	if loadSpec != nil && loadSpec.LoadAllColumns {
		load_all = true
	}

	count := 0
	skipped := 0
	broken_count := 0
	this_block := 0
	block_gc_time := time.Now().Sub(time.Now())

	broken_mutex := sync.Mutex{}
	broken_blocks := make([]string, 0)
	for f := range files {

		// TODO: decide more formally on order of block loading
		// SAMPLES: reverse chronological order
		// EVERYTHING ELSE: chronological order
		v := files[f]
		if OPTS.SAMPLES {
			v = files[len(files)-f-1]
		}

		if v.IsDir() && file_looks_like_block(v) {
			filename := path.Join(*FLAGS.DIR, t.Name, v.Name())
			this_block++

			wg.Add(1)
			go func() {
				defer wg.Done()

				start := time.Now()

				should_load := t.ShouldLoadBlockFromDir(filename, querySpec)

				if !should_load {
					skipped++
					return
				}

				block := t.LoadBlockFromDir(filename, loadSpec, load_all)
				if block == nil {
					broken_mutex.Lock()
					broken_blocks = append(broken_blocks, filename)
					broken_mutex.Unlock()
					return
				}

				if *DEBUG_FLAG {
					fmt.Fprint(os.Stderr, ".")
				}

				end := time.Now()
				if DEBUG_TIMING {
					if loadSpec != nil {
						Debug("LOADED BLOCK FROM DIR", filename, "TOOK", end.Sub(start))
					} else {
						Debug("LOADED INFO FOR BLOCK", filename, "TOOK", end.Sub(start))
					}
				}

				if len(block.RecordList) > 0 {

					if querySpec != nil { // Load and Query
						blockQuery := CopyQuerySpec(querySpec)
						blockCount := FilterAndAggRecords(blockQuery, &block.RecordList)

						if HOLD_MATCHES {
							block.Matched = blockQuery.Matched
						}

						m.Lock()
						count += blockCount
						block_specs[block.Name] = blockQuery
						m.Unlock()
					} else { // Just doing a regular old block load
						m.Lock()
						count += len(block.RecordList)
						m.Unlock()
					}

				}

				if OPTS.WRITE_BLOCK_INFO {
					block.SaveInfoToColumns(block.Name)
				}
				// don't delete when testing so we can verify block
				// loading results
				if loadSpec != nil && DELETE_BLOCKS_AFTER_QUERY && TEST_MODE == false {
					t.block_m.Lock()
					tb, ok := t.BlockList[block.Name]
					if ok {
						tb.RecycleSlab(loadSpec)

						delete(t.BlockList, block.Name)
					}
					t.block_m.Unlock()

				}
			}()

			if *FLAGS.SAMPLES {
				wg.Wait()

				if count > *FLAGS.LIMIT {
					break
				}
			}

			if DELETE_BLOCKS_AFTER_QUERY && this_block%CHUNKS_BEFORE_GC == 0 && *FLAGS.GC {
				wg.Wait()
				start := time.Now()

				if *FLAGS.RECYCLE_MEM == false {
					m.Lock()
					old_percent := debug.SetGCPercent(100)
					debug.SetGCPercent(old_percent)
					m.Unlock()
				}

				end := time.Now()
				if *DEBUG_FLAG {
					fmt.Fprint(os.Stderr, ",")
				}
				end = time.Now()
				block_gc_time += end.Sub(start)
			}
		}

	}

	rowStoreQuery := AfterLoadQueryCB{}
	var logend time.Time
	logstart := time.Now()
	if *FLAGS.READ_INGESTION_LOG {
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
		block_specs[INGEST_DIR] = rowStoreQuery.querySpec
		wg.Add(1)
		go func() {
			t.LoadRowStoreRecords(INGEST_DIR, rowStoreQuery.CB)
			m.Lock()
			logend = time.Now()
			m.Unlock()
		}()
	}

	wg.Wait()

	if *DEBUG_FLAG {
		fmt.Fprint(os.Stderr, "\n")
	}

	for _, broken_block_name := range broken_blocks {
		Debug("BLOCK", broken_block_name, "IS BROKEN, SKIPPING")
	}

	if *FLAGS.READ_INGESTION_LOG {
		m.Lock()
		Debug("LOADING & QUERYING INGESTION LOG TOOK", logend.Sub(logstart))
		Debug("INGESTION LOG RECORDS MATCHED", rowStoreQuery.count)
		m.Unlock()
		count += rowStoreQuery.count

		if DELETE_BLOCKS_AFTER_QUERY == false && t.RowBlock != nil {
			Debug("ROW STORE RECORD LENGTH IS", len(rowStoreQuery.records))
			t.RowBlock.RecordList = rowStoreQuery.records
			t.RowBlock.Matched = rowStoreQuery.records
		}
	}

	if block_gc_time > 0 {
		Debug("BLOCK GC TOOK", block_gc_time)
	}

	// RE-POPULATE LOOKUP TABLE INFO
	t.populate_string_id_lookup()

	Debug("SKIPPED", skipped, "BLOCKS BASED ON PRE FILTERS")
	Debug("SKIPPED", broken_count, "BLOCKS BASED ON BROKEN INFO")
	if FLAGS.LOAD_AND_QUERY != nil && *FLAGS.LOAD_AND_QUERY == true && querySpec != nil {
		// COMBINE THE PER BLOCK RESULTS
		astart := time.Now()
		resultSpec := CombineResults(querySpec, block_specs)

		aend := time.Now()
		Debug("AGGREGATING RESULT BLOCKS TOOK", aend.Sub(astart))

		querySpec.Cumulative = resultSpec.Cumulative

		querySpec.Results = resultSpec.Results
		querySpec.TimeResults = resultSpec.TimeResults

		SortResults(querySpec)
	}

	end := time.Now()

	if loadSpec != nil {
		Debug("LOADED", count, "RECORDS FROM BLOCKS INTO", t.Name, "TOOK", end.Sub(waystart))
	} else {
		Debug("INSPECTED", len(t.BlockList), "BLOCKS", "TOOK", end.Sub(waystart))
	}

	t.WriteBlockCache()

	return count

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
