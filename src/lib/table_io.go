package sybil

import "fmt"
import "log"
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
var DELETE_BLOCKS_AFTER_QUERY = true
var HOLD_MATCHES = false

type LoadSpec struct {
	columns        map[string]bool
	files          map[string]bool
	LoadAllColumns bool
	table          *Table
}

func NewLoadSpec() LoadSpec {
	l := LoadSpec{}
	l.files = make(map[string]bool)
	l.columns = make(map[string]bool)

	return l
}

func (t *Table) NewLoadSpec() LoadSpec {
	l := NewLoadSpec()
	l.table = t

	return l
}

func (l *LoadSpec) assert_col_type(name string, col_type int8) {
	if l.table == nil {
		return
	}
	name_id := l.table.get_key_id(name)
	log.Println("ASSERING COL TYPE", name, name_id, col_type, len(l.table.KeyTypes))

	if l.table.KeyTypes[name_id] == 0 {
		log.Fatal("Query Error! Column ", name, " does not exist")
	}

	if l.table.KeyTypes[name_id] != col_type {
		var col_type_name string
		switch col_type {
		case INT_VAL:
			col_type_name = "Int"
		case STR_VAL:
			col_type_name = "Str"
		case SET_VAL:
			col_type_name = "Set"
		}

		log.Fatal("Query Error! Key ", name, " exists, but is not of type ", col_type_name)
	}
}

func (l *LoadSpec) Str(name string) {
	l.assert_col_type(name, STR_VAL)
	l.columns[name] = true
	l.files["str_"+name+".db"] = true
}
func (l *LoadSpec) Int(name string) {
	l.assert_col_type(name, INT_VAL)
	l.columns[name] = true
	l.files["int_"+name+".db"] = true
}
func (l *LoadSpec) Set(name string) {
	l.assert_col_type(name, SET_VAL)
	l.columns[name] = true
	l.files["set_"+name+".db"] = true
}

func (t *Table) saveTableInfo(fname string) {
	var network bytes.Buffer // Stand-in for the network.
	filename := path.Join(*f_DIR, t.Name, fmt.Sprintf("%s.db", fname))

	// Create an encoder and send a value.
	enc := gob.NewEncoder(&network)
	err := enc.Encode(t)

	if err != nil {
		log.Fatal("encode:", err)
	}

	log.Println("SERIALIZED TABLE INFO", fname, "INTO ", network.Len(), "BYTES")

	w, _ := os.Create(filename)
	network.WriteTo(w)
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

	log.Println("SAVING RECORD LIST", len(records), t.Name)

	chunk_size := CHUNK_SIZE
	chunks := len(records) / chunk_size

	if chunks == 0 {
		filename, err := t.getNewIngestBlockName()
		if err != nil {
			log.Fatal("ERR SAVING BLOCK", filename, err)
		}
		t.SaveRecordsToBlock(records, filename)
	} else {
		for j := 0; j < chunks; j++ {
			filename, err := t.getNewIngestBlockName()
			if err != nil {
				log.Fatal("ERR SAVING BLOCK", filename, err)
			}
			t.SaveRecordsToBlock(records[j*chunk_size:(j+1)*chunk_size], filename)
		}

		// SAVE THE REMAINDER
		if len(records) > chunks*chunk_size {
			filename, err := t.getNewIngestBlockName()
			if err != nil {
				log.Fatal("Error creating new ingestion block", err)
			}

			t.SaveRecordsToBlock(records[chunks*chunk_size:], filename)
		}
	}

	return true
}

func (t *Table) SaveRecords() bool {
	os.MkdirAll(path.Join(*f_DIR, t.Name), 0777)
	col_id := t.get_key_id("time")

	sort.Sort(SortRecordsByTime{t.newRecords, col_id})

	t.FillPartialBlock()
	ret := t.saveRecordList(t.newRecords)
	t.newRecords = make(RecordList, 0)
	t.SaveTableInfo("info")

	return ret

}

func (t *Table) LoadTableInfo() {
	saved_table := Table{}
	start := time.Now()
	tablename := t.Name
	filename := path.Join(*f_DIR, tablename, "info.db")
	file, _ := os.Open(filename)
	log.Println("OPENING TABLE INFO FROM FILENAME", filename)
	dec := gob.NewDecoder(file)
	err := dec.Decode(&saved_table)
	end := time.Now()
	if err != nil {
		log.Println("TABLE INFO DECODE:", err)
		return
	}

	if DEBUG_TIMING {
		log.Println("TABLE INFO OPEN TOOK", end.Sub(start))
	}

	if len(saved_table.KeyTable) > 0 {
		t.KeyTable = saved_table.KeyTable
	}
	if len(saved_table.KeyTypes) > 0 {
		t.KeyTypes = saved_table.KeyTypes
	}

	if saved_table.IntInfo != nil {
		log.Println("LOADED CACHED INT INFO")
		t.IntInfo = saved_table.IntInfo
	}
	if saved_table.StrInfo != nil {
		log.Println("LOADED CACHED STR INFO")
		t.StrInfo = saved_table.StrInfo
	}

	t.populate_string_id_lookup()

	return
}

// Remove our pointer to the blocklist so a GC is triggered and
// a bunch of new memory becomes available
func (t *Table) ReleaseRecords() {
	t.BlockList = make(map[string]*TableBlock, 0)
	debug.FreeOSMemory()
}

func (t *Table) LoadAndQueryRecords(loadSpec *LoadSpec, querySpec *QuerySpec) int {
	waystart := time.Now()
	log.Println("LOADING", t.Name)

	files, _ := ioutil.ReadDir(path.Join(*f_DIR, t.Name))

	var wg sync.WaitGroup
	block_specs := make(map[string]*QuerySpec)

	wg.Add(1)
	go func() {
		defer wg.Done()
		t.LoadTableInfo()
	}()

	wg.Wait()

	m := &sync.Mutex{}

	load_all := false
	if loadSpec != nil && loadSpec.LoadAllColumns {
		load_all = true
	}

	count := 0
	skipped := 0
	block_count := 0
	this_block := 0
	block_gc_time := time.Now().Sub(time.Now())
	for f := range files {
		// Load blocks in reverse order
		v := files[len(files)-f-1]

		switch {
		case strings.HasSuffix(v.Name(), "info.db"):
			continue
		case strings.HasSuffix(v.Name(), "old"):
			continue
		case strings.HasSuffix(v.Name(), "partial"):
			continue
		}

		if v.IsDir() {
			filename := path.Join(*f_DIR, t.Name, v.Name())
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

				fmt.Fprint(os.Stderr, ".")

				end := time.Now()
				if DEBUG_TIMING {
					if loadSpec != nil {
						log.Println("LOADED BLOCK FROM DIR", filename, "TOOK", end.Sub(start))
					} else {
						log.Println("LOADED INFO FOR BLOCK", filename, "TOOK", end.Sub(start))
					}
				}

				if len(block.RecordList) > 0 {
					block_count++

					if querySpec != nil {
						blockQuery := CopyQuerySpec(querySpec)
						blockCount := FilterAndAggRecords(blockQuery, &block.RecordList)

						if HOLD_MATCHES {
							block.Matched = blockQuery.Matched
						}

						m.Lock()
						count += blockCount
						block_specs[block.Name] = blockQuery
						m.Unlock()
					} else {
						m.Lock()
						count += len(block.RecordList)
						m.Unlock()
					}

				}

				// don't delete when testing so we can verify block
				// loading results
				if loadSpec != nil && DELETE_BLOCKS_AFTER_QUERY && TEST_MODE == false {
					_, ok := t.BlockList[block.Name]
					if ok {
						delete(t.BlockList, block.Name)
					}
				}
			}()

			if *f_SAMPLES {
				wg.Wait()

				if count > *f_LIMIT {
					break
				}
			}

			if DELETE_BLOCKS_AFTER_QUERY && this_block%CHUNKS_BEFORE_GC == 0 && *f_GC {
				wg.Wait()
				m.Lock()
				start := time.Now()
				old_percent := debug.SetGCPercent(100)
				debug.SetGCPercent(old_percent)
				m.Unlock()
				end := time.Now()

				fmt.Fprint(os.Stderr, ",")
				end = time.Now()
				block_gc_time += end.Sub(start)
			}
		}

	}

	rowStoreQuery := AfterLoadQueryCB{}
	var logend time.Time
	logstart := time.Now()
	if *f_READ_INGESTION_LOG {
		if querySpec == nil {
			rowStoreQuery.querySpec = &QuerySpec{}
			rowStoreQuery.querySpec.Punctuate()
		} else {
			rowStoreQuery.querySpec = CopyQuerySpec(querySpec)
		}

		// Entrust AfterLoadQueryCB to call Done on wg
		rowStoreQuery.wg = &wg
		block_specs[INGEST_DIR] = rowStoreQuery.querySpec
		wg.Add(1)
		go func() {
			t.LoadRowStoreRecords(INGEST_DIR, rowStoreQuery.CB)
			logend = time.Now()
		}()
	}

	wg.Wait()

	fmt.Fprint(os.Stderr, "\n")

	if *f_READ_INGESTION_LOG {
		log.Println("LOADING & QUERYING INGESTION LOG TOOK", logend.Sub(logstart))
		log.Println("INGESTION LOG RECORDS MATCHED", rowStoreQuery.count)
		count += rowStoreQuery.count

		if HOLD_MATCHES {
			t.RowBlock.RecordList = rowStoreQuery.records
		}
	}

	if block_gc_time > 0 {
		log.Println("BLOCK GC TOOK", block_gc_time)
	}

	// RE-POPULATE LOOKUP TABLE INFO
	t.populate_string_id_lookup()

	log.Println("SKIPPED", skipped, "BLOCKS BASED ON PRE FILTERS")
	if f_LOAD_AND_QUERY != nil && *f_LOAD_AND_QUERY == true && querySpec != nil {
		// COMBINE THE PER BLOCK RESULTS
		astart := time.Now()
		resultSpec := CombineResults(querySpec, block_specs)

		aend := time.Now()
		log.Println("AGGREGATING RESULT BLOCKS TOOK", aend.Sub(astart))

		querySpec.Results = resultSpec.Results
		querySpec.TimeResults = resultSpec.TimeResults

		SortResults(querySpec)
	}

	end := time.Now()

	if loadSpec != nil {
		log.Println(count, "RECORDS LOADED FROM BLOCKS INTO", t.Name, "TOOK", end.Sub(waystart))
	} else {
		log.Println("INSPECTED", len(t.BlockList), "BLOCKS", "TOOK", end.Sub(waystart))
	}

	return count

}

func (t *Table) LoadRecords(loadSpec *LoadSpec) int {
	return t.LoadAndQueryRecords(loadSpec, nil)
}
