package sybil

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

func (t *Table) LoadAndQueryRecords(loadSpec *LoadSpec, querySpec *QuerySpec) int {
	waystart := time.Now()
	Debug("LOADING", t.Dir, t.Name)

	readRowsOnly := loadSpec != nil && loadSpec.ReadRowsOnly

	files, _ := ioutil.ReadDir(path.Join(t.Dir, t.Name))

	if readRowsOnly {
		Debug("ONLY READING RECORDS FROM ROW STORE")
		files = nil
	}

	if querySpec != nil {
		querySpec.Table = t
	}

	var wg sync.WaitGroup
	blockSpecs := make(map[string]*QuerySpec)
	toCacheSpecs := make(map[string]*QuerySpec)

	loadedInfo := t.LoadTableInfo()
	if !loadedInfo {
		if t.HasFlagFile() {
			return 0
		}
	}

	if loadSpec != nil && loadSpec.UpdateTableInfo {
		Debug("RESETTING TABLE INFO FOR OVERWRITING")
		t.IntInfo = make(IntInfoTable)
		t.StrInfo = make(StrInfoTable)
	}

	mu := &sync.Mutex{}

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
	var blockGcTime time.Duration

	allResults := make([]*QuerySpec, 0)

	brokenMu := sync.Mutex{}
	brokenBlocks := make([]string, 0)

	var memstats runtime.MemStats
	var maxAlloc = uint64(0)

	for f := range files {

		// TODO: decide more formally on order of block loading
		// SAMPLES: reverse chronological order
		// EVERYTHING ELSE: chronological order
		v := files[f]
		if querySpec != nil && querySpec.Samples {
			v = files[len(files)-f-1]
		}

		if v.IsDir() && fileLooksLikeBlock(v) {
			filename := path.Join(t.Dir, t.Name, v.Name())
			thisBlock++

			wg.Add(1)
			go func(t *Table) {
				defer wg.Done()

				start := time.Now()

				shouldLoad := t.ShouldLoadBlockFromDir(filename, querySpec)

				if !shouldLoad {
					skipped++
					return
				}

				var cachedSpec *QuerySpec
				var cachedBlock *TableBlock
				var replacements map[string]StrReplace

				if querySpec != nil {
					cachedBlock, cachedSpec = t.getCachedQueryForBlock(filename, querySpec)
					replacements = querySpec.StrReplace
				}

				var block *TableBlock
				if cachedSpec == nil {
					// couldnt load the cached query results
					block = t.LoadBlockFromDir(filename, loadSpec, replacements, loadAll)
					if block == nil {
						brokenMu.Lock()
						brokenBlocks = append(brokenBlocks, filename)
						brokenMu.Unlock()
						return
					}
				} else {
					// we are using cached query results
					block = cachedBlock
				}

				if *DEBUG {
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
						mu.Lock()
						count += len(block.RecordList)
						mu.Unlock()
					} else { // Load and Query
						blockQuery := cachedSpec
						if blockQuery == nil {
							blockQuery = CopyQuerySpec(querySpec)
							blockQuery.MatchedCount = FilterAndAggRecords(querySpec.HistogramParameters, blockQuery, &block.RecordList)

							if HOLD_MATCHES {
								block.Matched = blockQuery.Matched
							}

						}

						if blockQuery != nil {
							mu.Lock()
							if cachedSpec != nil {
								cachedCount += blockQuery.MatchedCount
								cachedBlocks++

							} else {
								count += blockQuery.MatchedCount
								loadedCount++
								if block.Info.NumRecords == int32(CHUNK_SIZE) {
									toCacheSpecs[block.Name] = blockQuery
								}
							}
							blockSpecs[block.Name] = blockQuery
							mu.Unlock()
						}
					}

				}

				if loadSpec != nil && loadSpec.WriteBlockInfo {
					block.SaveInfoToColumns(block.Name)
				}

				if querySpec != nil && querySpec.ExportTSV {
					block.ExportBlockData()
				}
				// don't delete when testing so we can verify block
				// loading results
				if loadSpec != nil && !loadSpec.SkipDeleteBlocksAfterQuery && !TEST_MODE {
					t.blockMu.Lock()
					tb, ok := t.BlockList[block.Name]
					if ok {
						tb.RecycleSlab(loadSpec)

						delete(t.BlockList, block.Name)
					}
					t.blockMu.Unlock()

				}
			}(t)

			if querySpec != nil && querySpec.Samples {
				wg.Wait()

				if count > querySpec.Limit {
					break
				}
			}
			// TODO remove flags.GC or move to loadSpec?

			if loadSpec != nil && !loadSpec.SkipDeleteBlocksAfterQuery && thisBlock%CHUNKS_BEFORE_GC == 0 {
				wg.Wait()
				start := time.Now()

				if !loadSpec.RecycleMemory {
					mu.Lock()
					oldPercent := debug.SetGCPercent(100)
					debug.SetGCPercent(oldPercent)
					mu.Unlock()
				}

				if querySpec != nil {

					if querySpec.CachedQueries {
						t.WriteQueryCache(toCacheSpecs)
					}
					toCacheSpecs = make(map[string]*QuerySpec)

					resultSpec := MultiCombineResults(querySpec, blockSpecs)
					blockSpecs = make(map[string]*QuerySpec)

					mu.Lock()
					allResults = append(allResults, resultSpec)
					mu.Unlock()

					runtime.ReadMemStats(&memstats)
					alloced := memstats.Alloc / 1024 / 1024
					if alloced > maxAlloc {
						maxAlloc = alloced
					}

					if alloced > 500 {
						debug.FreeOSMemory()
						runtime.ReadMemStats(&memstats)
					}
				}

				if *DEBUG {
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
	if loadSpec != nil && loadSpec.ReadIngestionLog {
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
		mu.Lock()
		blockSpecs[INGEST_DIR] = rowStoreQuery.querySpec
		mu.Unlock()

		wg.Add(1)
		go func(t *Table) {
			t.LoadRowStoreRecords(INGEST_DIR, loadSpec, rowStoreQuery.CB)
			mu.Lock()
			logend = time.Now()
			mu.Unlock()
		}(t)
	}

	wg.Wait()

	if *DEBUG {
		fmt.Fprint(os.Stderr, "\n")
	}

	for _, brokenBlockName := range brokenBlocks {
		Debug("BLOCK", brokenBlockName, "IS BROKEN, SKIPPING")
	}

	if loadSpec != nil && loadSpec.ReadIngestionLog {
		mu.Lock()
		Debug("LOADING & QUERYING INGESTION LOG TOOK", logend.Sub(logstart))
		Debug("INGESTION LOG RECORDS MATCHED", rowStoreQuery.count)
		mu.Unlock()
		count += rowStoreQuery.count

		if loadSpec != nil && loadSpec.SkipDeleteBlocksAfterQuery && t.RowBlock != nil {
			Debug("ROW STORE RECORD LENGTH IS", len(rowStoreQuery.records))
			t.RowBlock.RecordList = rowStoreQuery.records
			t.RowBlock.Matched = rowStoreQuery.records
		}
	}

	if blockGcTime > 0 {
		Debug("BLOCK GC TOOK", blockGcTime)
		Debug("MAX ALLOC", maxAlloc)
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
	if querySpec != nil && querySpec.CachedQueries {
		t.WriteQueryCache(toCacheSpecs)
	}

	if querySpec != nil {
		// COMBINE THE PER BLOCK RESULTS
		astart := time.Now()
		for k, v := range allResults {
			blockSpecs[fmt.Sprintf("result_%v", k)] = v
		}

		resultSpec := MultiCombineResults(querySpec, blockSpecs)

		aend := time.Now()
		Debug("AGGREGATING RESULT BLOCKS TOOK", aend.Sub(astart))

		querySpec.Cumulative = resultSpec.Cumulative

		querySpec.Results = resultSpec.Results
		querySpec.TimeResults = resultSpec.TimeResults
		querySpec.MatchedCount = count + cachedCount

		querySpec.SortResults(querySpec.OrderBy)
	}

	t.WriteBlockCache()

	return count

}
