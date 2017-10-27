package sybil

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/metadata"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/query/aggregate"
	. "github.com/logv/sybil/src/query/filters"
	. "github.com/logv/sybil/src/query/printer"
	. "github.com/logv/sybil/src/query/query_cache"
	. "github.com/logv/sybil/src/query/slab_manager"
	. "github.com/logv/sybil/src/query/specs"
	. "github.com/logv/sybil/src/storage/column_store"
	. "github.com/logv/sybil/src/storage/file_locks"
	. "github.com/logv/sybil/src/storage/metadata_io"
	. "github.com/logv/sybil/src/storage/row_store"
)

func LoadAndQueryRecords(t *Table, loadSpec *LoadSpec, querySpec *QuerySpec) int {
	waystart := time.Now()
	Debug("LOADING", *FLAGS.DIR, t.Name)

	files, _ := ioutil.ReadDir(path.Join(*FLAGS.DIR, t.Name))

	if OPTS.READ_ROWS_ONLY {
		Debug("ONLY READING RECORDS FROM ROW STORE")
		files = nil
	}

	if querySpec != nil {

		querySpec.Table = t
	}

	// Load and setup our OPTS.STR_REPLACEMENTS
	OPTS.STR_REPLACEMENTS = make(map[string]StrReplace)
	if FLAGS.STR_REPLACE != nil {
		var replacements = strings.Split(*FLAGS.STR_REPLACE, *FLAGS.FIELD_SEPARATOR)
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
	to_cache_specs := make(map[string]*QuerySpec)

	loaded_info := LoadTableInfo(t)
	if loaded_info == false {
		if HasFlagFile(t) {
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
		if OPTS.SAMPLES {
			v = files[len(files)-f-1]
		}

		if v.IsDir() && FileLooksLikeBlock(v) {
			filename := path.Join(*FLAGS.DIR, t.Name, v.Name())
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
					cachedBlock, cachedSpec = GetCachedQueryForBlock(t, filename, querySpec)
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

				if *FLAGS.DEBUG {
					if cachedSpec != nil {
						fmt.Fprint(os.Stderr, "c")
					} else {
						fmt.Fprint(os.Stderr, ".")

					}
				}

				end := time.Now()
				if DEBUG_TIMING {
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

							if OPTS.HOLD_MATCHES {
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

				if OPTS.WRITE_BLOCK_INFO {
					SaveBlockInfo(block, block.Name)
				}

				if *FLAGS.EXPORT {
					ExportBlockData(block)
				}
				// don't delete when testing so we can verify block
				// loading results
				if loadSpec != nil && OPTS.DELETE_BLOCKS_AFTER_QUERY && TEST_MODE == false {
					t.BlockMutex.Lock()
					tb, ok := t.BlockList[block.Name]
					if ok {
						RecycleSlab(tb, loadSpec)

						delete(t.BlockList, block.Name)
					}
					t.BlockMutex.Unlock()

				}
			}()

			if *FLAGS.SAMPLES {
				wg.Wait()

				if count > *FLAGS.LIMIT {
					break
				}
			}

			if OPTS.DELETE_BLOCKS_AFTER_QUERY && this_block%CHUNKS_BEFORE_GC == 0 && *FLAGS.GC {
				wg.Wait()
				start := time.Now()

				if *FLAGS.RECYCLE_MEM == false {
					m.Lock()
					old_percent := debug.SetGCPercent(100)
					debug.SetGCPercent(old_percent)
					m.Unlock()
				}

				if *FLAGS.DEBUG {
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
	if *FLAGS.READ_INGESTION_LOG {
		if querySpec == nil {
			rowStoreQuery.QuerySpec = &QuerySpec{}
			rowStoreQuery.QuerySpec.Table = t
			rowStoreQuery.QuerySpec.Punctuate()
		} else {
			rowStoreQuery.QuerySpec = CopyQuerySpec(querySpec)
			rowStoreQuery.QuerySpec.Table = t
		}

		// Entrust AfterLoadQueryCB to call Done on wg
		rowStoreQuery.WG = &wg
		m.Lock()
		block_specs[INGEST_DIR] = rowStoreQuery.QuerySpec
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

	if *FLAGS.DEBUG {
		fmt.Fprint(os.Stderr, "\n")
	}

	for _, broken_block_name := range broken_blocks {
		Debug("BLOCK", broken_block_name, "IS BROKEN, SKIPPING")
	}

	if *FLAGS.READ_INGESTION_LOG {
		m.Lock()
		Debug("LOADING & QUERYING INGESTION LOG TOOK", logend.Sub(logstart))
		Debug("INGESTION LOG RECORDS MATCHED", rowStoreQuery.Count)
		m.Unlock()
		count += rowStoreQuery.Count
		Debug("STORE BLOCKS AFTER QUERY", OPTS.DELETE_BLOCKS_AFTER_QUERY)

		if OPTS.DELETE_BLOCKS_AFTER_QUERY == false && t.RowBlock != nil {
			Debug("ROW STORE RECORD LENGTH IS", len(rowStoreQuery.Records))
			t.RowBlock.RecordList = rowStoreQuery.Records
			t.RowBlock.Matched = rowStoreQuery.Records
		}
	}

	if block_gc_time > 0 {
		Debug("BLOCK GC TOOK", block_gc_time)
	}

	// RE-POPULATE LOOKUP TABLE INFO
	PopulateStringIDLookup(t)

	Debug("SKIPPED", skipped, "BLOCKS BASED ON PRE FILTERS")
	Debug("SKIPPED", broken_count, "BLOCKS BASED ON BROKEN INFO")
	Debug("SKIPPED", cached_blocks, "BLOCKS &", cached_count, "RECORDS BASED ON QUERY CACHE")
	end := time.Now()
	if loadSpec != nil {
		Debug("LOADED", count, "RECORDS FROM", loaded_count, "BLOCKS INTO", t.Name, "TOOK", end.Sub(waystart))
	} else {
		Debug("INSPECTED", len(t.BlockList), "BLOCKS", "TOOK", end.Sub(waystart))
	}

	// NOTE: we have to write the query cache before we combine our results,
	// bc combining results is not idempotent
	WriteQueryCache(t, to_cache_specs)

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

	WriteBlockCache(t)

	return count

}

func LoadRecords(t *Table, loadSpec *LoadSpec) int {
	LoadBlockCache(t)

	return LoadAndQueryRecords(t, loadSpec, nil)
}

func init() {
	SetTableQueryFunc(LoadRecords)
}
