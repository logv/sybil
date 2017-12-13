package sybil

import "fmt"

import "os"
import "path"
import "strings"
import "sync"
import "time"
import "io/ioutil"
import "runtime"
import "runtime/debug"

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

		if querySpec != nil {
			querySpec.StrReplace = OPTS.STR_REPLACEMENTS
		}
	}

	var wg sync.WaitGroup
	block_specs := make(map[string]*QuerySpec)
	to_cache_specs := make(map[string]*QuerySpec)

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
	cached_count := 0
	cached_blocks := 0
	loaded_count := 0
	skipped := 0
	broken_count := 0
	this_block := 0
	block_gc_time := time.Now().Sub(time.Now())

	all_results := make([]*QuerySpec, 0)

	broken_mutex := sync.Mutex{}
	broken_blocks := make([]string, 0)

	var memstats runtime.MemStats
	var max_alloc = uint64(0)

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

				var cachedSpec *QuerySpec
				var cachedBlock *TableBlock

				if querySpec != nil {
					cachedBlock, cachedSpec = t.getCachedQueryForBlock(filename, querySpec)
				}

				var block *TableBlock
				if cachedSpec == nil {
					// couldnt load the cached query results
					block = t.LoadBlockFromDir(filename, loadSpec, load_all)
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

							if HOLD_MATCHES {
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
					block.SaveInfoToColumns(block.Name)
				}

				if *FLAGS.EXPORT {
					block.ExportBlockData()
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

				if querySpec != nil {

					t.WriteQueryCache(to_cache_specs)
					to_cache_specs = make(map[string]*QuerySpec)

					resultSpec := MultiCombineResults(querySpec, block_specs)
					block_specs = make(map[string]*QuerySpec)

					m.Lock()
					all_results = append(all_results, resultSpec)
					m.Unlock()

					runtime.ReadMemStats(&memstats)
					alloced := memstats.Alloc / 1024 / 1024
					if alloced > max_alloc {
						max_alloc = alloced
					}

					if alloced > 500 {
						debug.FreeOSMemory()
						runtime.ReadMemStats(&memstats)
					}
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
			t.LoadRowStoreRecords(INGEST_DIR, rowStoreQuery.CB)
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
		Debug("MAX ALLOC", max_alloc)
	}

	// RE-POPULATE LOOKUP TABLE INFO
	t.populate_string_id_lookup()

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
	t.WriteQueryCache(to_cache_specs)

	if FLAGS.LOAD_AND_QUERY != nil && *FLAGS.LOAD_AND_QUERY == true && querySpec != nil {
		// COMBINE THE PER BLOCK RESULTS
		astart := time.Now()
		for k, v := range all_results {
			block_specs[fmt.Sprintf("result_%v", k)] = v
		}

		resultSpec := MultiCombineResults(querySpec, block_specs)

		aend := time.Now()
		Debug("AGGREGATING RESULT BLOCKS TOOK", aend.Sub(astart))

		querySpec.Cumulative = resultSpec.Cumulative

		querySpec.Results = resultSpec.Results
		querySpec.TimeResults = resultSpec.TimeResults
		querySpec.MatchedCount = count + cached_count

		querySpec.SortResults(querySpec.OrderBy)
	}

	t.WriteBlockCache()

	return count

}
