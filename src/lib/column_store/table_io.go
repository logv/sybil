package sybil

import (
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	. "github.com/logv/sybil/src/lib/aggregate"
	. "github.com/logv/sybil/src/lib/block_manager"
	"github.com/logv/sybil/src/lib/common"
	"github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/encoders"
	. "github.com/logv/sybil/src/lib/locks"
	. "github.com/logv/sybil/src/lib/metadata"
	. "github.com/logv/sybil/src/lib/printer"
	. "github.com/logv/sybil/src/lib/row_store"
	. "github.com/logv/sybil/src/lib/specs"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/lib/table_info"
)

var CHUNKS_BEFORE_GC = 16

var DELETE_BLOCKS_AFTER_QUERY = true
var BLOCKS_PER_CACHE_FILE = 64

func GetNewIngestBlockName(t *Table) (string, error) {
	common.Debug("GETTING INGEST BLOCK NAME", *config.FLAGS.DIR, "TABLE", t.Name)
	name, err := ioutil.TempDir(path.Join(*config.FLAGS.DIR, t.Name), "block")
	return name, err
}

func saveRecordList(t *Table, records RecordList) bool {
	if len(records) == 0 {
		return false
	}

	common.Debug("SAVING RECORD LIST", len(records), t.Name)

	chunk_size := CHUNK_SIZE
	chunks := len(records) / chunk_size

	if chunks == 0 {
		filename, err := GetNewIngestBlockName(t)
		if err != nil {
			common.Error("ERR SAVING BLOCK", filename, err)
		}
		SaveRecordsToBlock(t, records, filename)
	} else {
		for j := 0; j < chunks; j++ {
			filename, err := GetNewIngestBlockName(t)
			if err != nil {
				common.Error("ERR SAVING BLOCK", filename, err)
			}
			SaveRecordsToBlock(t, records[j*chunk_size:(j+1)*chunk_size], filename)
		}

		// SAVE THE REMAINDER
		if len(records) > chunks*chunk_size {
			filename, err := GetNewIngestBlockName(t)
			if err != nil {
				common.Error("common.Error creating new ingestion block", err)
			}

			SaveRecordsToBlock(t, records[chunks*chunk_size:], filename)
		}
	}

	return true
}

func SaveRecordsToColumns(t *Table) bool {
	os.MkdirAll(path.Join(*config.FLAGS.DIR, t.Name), 0777)
	sort.Sort(SortRecordsByTime{t.NewRecords})

	FillPartialBlock(t)
	ret := saveRecordList(t, t.NewRecords)
	t.NewRecords = make(RecordList, 0)
	SaveTableInfo(t, "info")

	return ret

}

func HasFlagFile(t *Table) bool {
	// Make a determination of whether this is a new table or not. if it is a
	// new table, we are fine, but if it's not - we are in trouble!
	flagfile := path.Join(*config.FLAGS.DIR, t.Name, "info.db.exists")
	_, err := os.Open(flagfile)
	// If the flagfile exists and we couldn't read the file info, we are in trouble!
	if err == nil {
		ReleaseInfoLock(t)
		common.Warn("Table info missing, but flag file exists!")
		return true
	}

	return false

}

func FileLooksLikeBlock(v os.FileInfo) bool {

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

func getNewCacheBlockFile(t *Table) (*os.File, error) {
	common.Debug("GETTING CACHE BLOCK NAME", *config.FLAGS.DIR, "TABLE", t.Name)
	table_cache_dir := path.Join(*config.FLAGS.DIR, t.Name, CACHE_DIR)
	os.MkdirAll(table_cache_dir, 0755)

	// this info block needs to be moved once its finished being written to
	file, err := ioutil.TempFile(table_cache_dir, "info")
	return file, err
}

func LoadBlockCache(t *Table) {
	if GrabCacheLock(t) == false {
		return
	}

	defer ReleaseCacheLock(t)
	files, err := ioutil.ReadDir(path.Join(*config.FLAGS.DIR, t.Name, CACHE_DIR))

	if err != nil {
		return
	}

	for _, block_file := range files {
		filename := path.Join(*config.FLAGS.DIR, t.Name, CACHE_DIR, block_file.Name())
		block_cache := SavedBlockCache{}
		if err != nil {
			continue
		}

		err = DecodeInto(filename, &block_cache)
		if err != nil {
			continue
		}

		for k, v := range block_cache {
			t.BlockInfoCache[k] = v
		}
	}

	common.Debug("FILLED BLOCK CACHE WITH", len(t.BlockInfoCache), "ITEMS")
}

func WriteQueryCache(t *Table, to_cache_specs map[string]*QuerySpec) {

	// NOW WE SAVE OUR QUERY CACHE HERE...
	savestart := time.Now()

	if *config.FLAGS.CACHED_QUERIES {
		for blockName, blockQuery := range to_cache_specs {

			if blockName == INGEST_DIR {
				continue
			}

			SaveCachedResults(blockQuery, blockName)
			if *config.FLAGS.DEBUG {
				fmt.Fprint(os.Stderr, "s")
			}
		}

		saveend := time.Now()

		if len(to_cache_specs) > 0 {
			if *config.FLAGS.DEBUG {
				fmt.Fprint(os.Stderr, "\n")
			}
			common.Debug("SAVING CACHED QUERIES TOOK", saveend.Sub(savestart))
		}
	}

	// END QUERY CACHE SAVING

}

func WriteBlockCache(t *Table) {
	if len(t.NewBlockInfos) == 0 {
		return
	}

	if GrabCacheLock(t) == false {
		return
	}

	defer ReleaseCacheLock(t)

	common.Debug("WRITING BLOCK CACHE, OUTSTANDING", len(t.NewBlockInfos))

	var num_blocks = len(t.NewBlockInfos) / BLOCKS_PER_CACHE_FILE

	for i := 0; i < num_blocks; i++ {
		cached_info := t.NewBlockInfos[i*BLOCKS_PER_CACHE_FILE : (i+1)*BLOCKS_PER_CACHE_FILE]

		block_file, err := getNewCacheBlockFile(t)
		if err != nil {
			common.Debug("TROUBLE CREATING CACHE BLOCK FILE")
			break
		}
		block_cache := SavedBlockCache{}

		for _, block_name := range cached_info {
			block_cache[block_name] = t.BlockInfoCache[block_name]
		}

		enc := gob.NewEncoder(block_file)
		err = enc.Encode(&block_cache)
		if err != nil {
			common.Debug("ERROR ENCODING BLOCK CACHE", err)
		}

		pathname := fmt.Sprintf("%s.db", block_file.Name())

		common.Debug("RENAMING", block_file.Name(), pathname)
		common.RenameAndMod(block_file.Name(), pathname)

	}

	t.NewBlockInfos = t.NewBlockInfos[:0]

}

func LoadAndQueryRecords(t *Table, loadSpec *LoadSpec, querySpec *QuerySpec) int {
	waystart := time.Now()
	common.Debug("LOADING", *config.FLAGS.DIR, t.Name)

	files, _ := ioutil.ReadDir(path.Join(*config.FLAGS.DIR, t.Name))

	if config.OPTS.READ_ROWS_ONLY {
		common.Debug("ONLY READING RECORDS FROM ROW STORE")
		files = nil
	}

	if querySpec != nil {

		querySpec.Table = t
	}

	// Load and setup our config.OPTS.STR_REPLACEMENTS
	config.OPTS.STR_REPLACEMENTS = make(map[string]config.StrReplace)
	if config.FLAGS.STR_REPLACE != nil {
		var replacements = strings.Split(*config.FLAGS.STR_REPLACE, *config.FLAGS.FIELD_SEPARATOR)
		for _, repl := range replacements {
			tokens := strings.Split(repl, ":")
			if len(tokens) > 2 {
				col := tokens[0]
				pattern := tokens[1]
				replacement := tokens[2]
				config.OPTS.STR_REPLACEMENTS[col] = config.StrReplace{pattern, replacement}
			}
		}
	}

	var wg sync.WaitGroup
	block_specs := make(map[string]*QuerySpec)
	to_cache_specs := make(map[string]*QuerySpec)

	loaded_info := LoadTableInfo(t)
	if loaded_info == false {
		if HasFlagFile(t) {
			return 0
		}
	}

	if *config.FLAGS.UPDATE_TABLE_INFO {
		common.Debug("RESETTING TABLE INFO FOR OVERWRITING")
		t.IntInfo = make(IntInfoTable)
		t.StrInfo = make(StrInfoTable)
	}

	m := &sync.Mutex{}

	load_all := false
	if loadSpec != nil && loadSpec.LoadAllColumns {
		load_all = true
	}

	count := 0
	cached_count := 0
	cached_blocks := 0
	loaded_count := 0
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
		if config.OPTS.SAMPLES {
			v = files[len(files)-f-1]
		}

		if v.IsDir() && FileLooksLikeBlock(v) {
			filename := path.Join(*config.FLAGS.DIR, t.Name, v.Name())
			this_block++

			wg.Add(1)
			go func() {
				defer wg.Done()

				start := time.Now()

				should_load := ShouldLoadBlockFromDir(t, filename, querySpec)

				if !should_load {
					skipped++
					return
				}

				var cachedSpec *QuerySpec
				var cachedBlock *TableBlock

				if querySpec != nil {
					cachedBlock, cachedSpec = getCachedQueryForBlock(t, filename, querySpec)
				}

				var block *TableBlock
				if cachedSpec == nil {
					// couldnt load the cached query results
					block = LoadBlockFromDir(t, filename, loadSpec, load_all)
					if block == nil {
						broken_mutex.Lock()
						broken_blocks = append(broken_blocks, filename)
						broken_mutex.Unlock()
						return
					}
				} else {
					// we are using cached query results
					block = cachedBlock
				}

				if *config.FLAGS.DEBUG {
					if cachedSpec != nil {
						fmt.Fprint(os.Stderr, "c")
					} else {
						fmt.Fprint(os.Stderr, ".")

					}
				}

				end := time.Now()
				if DEBUG_TIMING {
					if loadSpec != nil {
						common.Debug("LOADED BLOCK FROM DIR", filename, "TOOK", end.Sub(start))
					} else {
						common.Debug("LOADED INFO FOR BLOCK", filename, "TOOK", end.Sub(start))
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

							if config.OPTS.HOLD_MATCHES {
								block.Matched = blockQuery.Matched
							}

						}

						if blockQuery != nil {
							m.Lock()
							if cachedSpec != nil {
								cached_count += blockQuery.MatchedCount
								cached_blocks += 1

							} else {
								count += blockQuery.MatchedCount
								loaded_count += 1
								if block.Info.NumRecords == int32(CHUNK_SIZE) {
									to_cache_specs[block.Name] = blockQuery
								}
							}
							block_specs[block.Name] = blockQuery
							m.Unlock()
						}
					}

				}

				if config.OPTS.WRITE_BLOCK_INFO {
					SaveInfoToColumns(block, block.Name)
				}

				if *config.FLAGS.EXPORT {
					ExportBlockData(block)
				}
				// don't delete when testing so we can verify block
				// loading results
				if loadSpec != nil && DELETE_BLOCKS_AFTER_QUERY && config.TEST_MODE == false {
					t.BlockMutex.Lock()
					tb, ok := t.BlockList[block.Name]
					if ok {
						RecycleSlab(tb, loadSpec)

						delete(t.BlockList, block.Name)
					}
					t.BlockMutex.Unlock()

				}
			}()

			if *config.FLAGS.SAMPLES {
				wg.Wait()

				if count > *config.FLAGS.LIMIT {
					break
				}
			}

			if DELETE_BLOCKS_AFTER_QUERY && this_block%CHUNKS_BEFORE_GC == 0 && *config.FLAGS.GC {
				wg.Wait()
				start := time.Now()

				if *config.FLAGS.RECYCLE_MEM == false {
					m.Lock()
					old_percent := debug.SetGCPercent(100)
					debug.SetGCPercent(old_percent)
					m.Unlock()
				}

				if *config.FLAGS.DEBUG {
					fmt.Fprint(os.Stderr, ",")
				}
				end := time.Now()
				block_gc_time += end.Sub(start)
			}
		}

	}

	rowStoreQuery := AfterLoadQueryCB{}
	var logend time.Time
	logstart := time.Now()
	if *config.FLAGS.READ_INGESTION_LOG {
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
		block_specs[INGEST_DIR] = rowStoreQuery.querySpec
		m.Unlock()

		wg.Add(1)
		go func() {
			LoadRowStoreRecords(t, INGEST_DIR, rowStoreQuery.CB)
			m.Lock()
			logend = time.Now()
			m.Unlock()
		}()
	}

	wg.Wait()

	if *config.FLAGS.DEBUG {
		fmt.Fprint(os.Stderr, "\n")
	}

	for _, broken_block_name := range broken_blocks {
		common.Debug("BLOCK", broken_block_name, "IS BROKEN, SKIPPING")
	}

	if *config.FLAGS.READ_INGESTION_LOG {
		m.Lock()
		common.Debug("LOADING & QUERYING INGESTION LOG TOOK", logend.Sub(logstart))
		common.Debug("INGESTION LOG RECORDS MATCHED", rowStoreQuery.count)
		m.Unlock()
		count += rowStoreQuery.count

		if DELETE_BLOCKS_AFTER_QUERY == false && t.RowBlock != nil {
			common.Debug("ROW STORE RECORD LENGTH IS", len(rowStoreQuery.records))
			t.RowBlock.RecordList = rowStoreQuery.records
			t.RowBlock.Matched = rowStoreQuery.records
		}
	}

	if block_gc_time > 0 {
		common.Debug("BLOCK GC TOOK", block_gc_time)
	}

	// RE-POPULATE LOOKUP TABLE INFO
	PopulateStringIDLookup(t)

	common.Debug("SKIPPED", skipped, "BLOCKS BASED ON PRE FILTERS")
	common.Debug("SKIPPED", broken_count, "BLOCKS BASED ON BROKEN INFO")
	common.Debug("SKIPPED", cached_blocks, "BLOCKS &", cached_count, "RECORDS BASED ON QUERY CACHE")
	end := time.Now()
	if loadSpec != nil {
		common.Debug("LOADED", count, "RECORDS FROM", loaded_count, "BLOCKS INTO", t.Name, "TOOK", end.Sub(waystart))
	} else {
		common.Debug("INSPECTED", len(t.BlockList), "BLOCKS", "TOOK", end.Sub(waystart))
	}

	// NOTE: we have to write the query cache before we combine our results,
	// bc combining results is not idempotent
	WriteQueryCache(t, to_cache_specs)

	if config.FLAGS.LOAD_AND_QUERY != nil && *config.FLAGS.LOAD_AND_QUERY == true && querySpec != nil {
		// COMBINE THE PER BLOCK RESULTS
		astart := time.Now()
		resultSpec := CombineResults(querySpec, block_specs)

		aend := time.Now()
		common.Debug("AGGREGATING RESULT BLOCKS TOOK", aend.Sub(astart))

		querySpec.Cumulative = resultSpec.Cumulative

		querySpec.Results = resultSpec.Results
		querySpec.TimeResults = resultSpec.TimeResults

		SortResults(querySpec)
	}

	WriteBlockCache(t)

	return count

}

func LoadRecords(t *Table, loadSpec *LoadSpec) int {
	LoadBlockCache(t)

	return LoadAndQueryRecords(t, loadSpec, nil)
}

func ChunkAndSave(t *Table) {

	if len(t.NewRecords) >= CHUNK_SIZE {
		os.MkdirAll(path.Join(*config.FLAGS.DIR, t.Name), 0777)
		name, err := GetNewIngestBlockName(t)
		if err == nil {
			SaveRecordsToBlock(t, t.NewRecords, name)
			SaveTableInfo(t, "info")
			t.NewRecords = make(RecordList, 0)
			ReleaseRecords(t)
		} else {
			common.Error("ERROR SAVING BLOCK", err)
		}
	}

}

func IsNotExist(t *Table) bool {
	table_dir := path.Join(*config.FLAGS.DIR, t.Name)
	_, err := ioutil.ReadDir(table_dir)
	return err != nil
}
