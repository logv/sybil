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

	"github.com/pkg/errors"
)

func (t *Table) LoadAndQueryRecords(loadSpec *LoadSpec, querySpec *QuerySpec) (int, error) {
	waystart := time.Now()
	Debug("LOADING", FLAGS.DIR, t.Name)

	readRowsOnly := loadSpec != nil && loadSpec.ReadRowsOnly

	files, _ := ioutil.ReadDir(path.Join(FLAGS.DIR, t.Name))

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

	if err := t.LoadTableInfo(); err != nil {
		if t.HasFlagFile() {
			return 0, errors.Wrap(err, "issue loading existing table")
		}
	}

	if FLAGS.UPDATE_TABLE_INFO {
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
	type brokenBlock struct {
		name string
		err  error
	}
	brokenBlocks := make([]brokenBlock, 0)

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
			filename := path.Join(FLAGS.DIR, t.Name, v.Name())
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
					var replacements map[string]StrReplace
					if querySpec != nil {
						replacements = querySpec.StrReplace
					}
					var err error
					block, err = t.LoadBlockFromDir(filename, loadSpec, loadAll, replacements)
					if block == nil || err != nil {
						brokenMu.Lock()
						brokenBlocks = append(brokenBlocks, brokenBlock{
							name: filename,
							err:  err,
						})
						brokenMu.Unlock()
						return
					}
				} else {
					// we are using cached query results
					block = cachedBlock
				}

				if FLAGS.DEBUG {
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
							blockQuery.MatchedCount = FilterAndAggRecords(blockQuery, &block.RecordList)

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

				if FLAGS.WRITE_BLOCK_INFO {
					block.SaveInfoToColumns(block.Name)
				}

				if FLAGS.EXPORT {
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
			}()

			if querySpec != nil && querySpec.Samples {
				wg.Wait()

				if count > querySpec.Limit {
					break
				}
			}

			if loadSpec != nil && !loadSpec.SkipDeleteBlocksAfterQuery && thisBlock%CHUNKS_BEFORE_GC == 0 && FLAGS.GC {
				wg.Wait()
				start := time.Now()

				if !FLAGS.RECYCLE_MEM {
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

				if FLAGS.DEBUG {
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
	if FLAGS.READ_INGESTION_LOG {
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
		go func() {
			t.LoadRowStoreRecords(INGEST_DIR, rowStoreQuery.CB)
			mu.Lock()
			logend = time.Now()
			mu.Unlock()
		}()
	}

	wg.Wait()

	if FLAGS.DEBUG {
		fmt.Fprint(os.Stderr, "\n")
	}

	for _, brokenBlockName := range brokenBlocks {
		Debug("BLOCK", brokenBlockName, "IS BROKEN, SKIPPING")
	}

	if FLAGS.READ_INGESTION_LOG {
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
	t.WriteQueryCache(toCacheSpecs)

	if FLAGS.LOAD_AND_QUERY && querySpec != nil {
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

	return count, nil
}
