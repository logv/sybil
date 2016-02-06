package pcs

import "log"
import "fmt"
import "time"
import "os"
import "strings"
import "bytes"
import "io/ioutil"
import "encoding/gob"
import "sync"
import "sort"
import "path"
import "strconv"

var DEBUG_TIMING = false

type LoadSpec struct {
	columns        map[string]bool
	LoadAllColumns bool
	table          *Table
}

func NewLoadSpec() LoadSpec {
	l := LoadSpec{}
	l.columns = make(map[string]bool)

	return l
}

func (t *Table) NewLoadSpec() LoadSpec {
	l := LoadSpec{}
	l.table = t
	l.columns = make(map[string]bool)

	return l
}

func (l *LoadSpec) assert_col_type(name string, col_type int) {
	if l.table == nil {
		return
	}

	name_id := l.table.get_key_id(name)
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
	l.columns["str_"+name+".db"] = true
}
func (l *LoadSpec) Int(name string) {
	l.assert_col_type(name, INT_VAL)
	l.columns["int_"+name+".db"] = true
}
func (l *LoadSpec) Set(name string) {
	l.assert_col_type(name, SET_VAL)
	l.columns["set_"+name+".db"] = true
}

// Helpers for block directory structure
func getBlockName(id int) string {
	return fmt.Sprintf("digest%05s", strconv.FormatInt(int64(id), 10))
}

func getBlockDir(name string, id int) string {
	return path.Join(*f_DIR, name, getBlockName(id))
}
func getBlockFilename(name string, id int) string {
	return path.Join(*f_DIR, name, fmt.Sprintf("%05s.db", getBlockName(id)))
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

func (t *Table) SaveRecordsToBlock(records []*Record, filename string) {
	if len(records) == 0 {
		return
	}

	temp_block := newTableBlock()
	temp_block.RecordList = records
	temp_block.table = t

	temp_block.SaveToColumns(filename)
}

func (t *Table) FillPartialBlock() bool {
	if len(t.newRecords) == 0 {
		return false
	}

	log.Println("CHECKING FOR PARTIAL BLOCK", t.LastBlockId)

	// Open up our last record block, see how full it is
	filename := getBlockDir(t.Name, t.LastBlockId)

	block := t.LoadBlockFromDir(filename, nil, true /* LOAD ALL RECORDS */)
	partialRecords := block.RecordList
	log.Println("LAST BLOCK HAS", len(partialRecords), "RECORDS")

	incBlockId := false
	if len(partialRecords) < CHUNK_SIZE {
		delta := CHUNK_SIZE - len(partialRecords)
		if delta > len(t.newRecords) {
			delta = len(t.newRecords)
		} else {
			incBlockId = true
		}

		log.Println("SAVING PARTIAL RECORDS", delta, "TO", filename)
		partialRecords = append(partialRecords, t.newRecords[0:delta]...)
		t.SaveRecordsToBlock(partialRecords, filename)
		if delta < len(t.newRecords) {
			t.newRecords = t.newRecords[delta:]
		} else {
			t.newRecords = make([]*Record, 0)
		}

	} else {
		incBlockId = true
	}

	if incBlockId {
		t.LastBlockId++
	}

	return true
}

func getSaveTable(t *Table) *Table {
	return &Table{Name: t.Name,
		KeyTable:    t.KeyTable,
		KeyTypes:    t.KeyTypes,
		IntInfo:     t.IntInfo,
		StrInfo:     t.StrInfo,
		LastBlockId: t.LastBlockId}
}

func (t *Table) saveRecordList(records []*Record) bool {
	if len(records) == 0 {
		return false
	}

	log.Println("SAVING RECORD LIST", len(records), t.Name)

	chunk_size := CHUNK_SIZE
	chunks := len(records) / chunk_size

	if chunks == 0 {
		filename := getBlockFilename(t.Name, t.LastBlockId)
		t.SaveRecordsToBlock(records, filename)
	} else {
		for j := 0; j < chunks; j++ {
			filename := getBlockFilename(t.Name, t.LastBlockId)
			t.SaveRecordsToBlock(records[j*chunk_size:(j+1)*chunk_size], filename)
			t.LastBlockId++
		}

		// SAVE THE REMAINDER
		if len(records) > chunks*chunk_size {
			filename := getBlockFilename(t.Name, t.LastBlockId)
			t.SaveRecordsToBlock(records[chunks*chunk_size:], filename)
		}
	}

	log.Println("LAST TABLE BLOCK IS", t.LastBlockId)

	return true
}

func (t *Table) SaveRecords() bool {
	os.MkdirAll(path.Join(*f_DIR, t.Name), 0777)
	col_id := t.get_key_id("time")

	sort.Sort(SortRecordsByTime{t.newRecords, col_id})

	t.FillPartialBlock()
	ret := t.saveRecordList(t.newRecords)
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

	t.LastBlockId = saved_table.LastBlockId

	t.populate_string_id_lookup()

	return
}

// TODO: have this only pull the blocks into column format and not materialize
// the columns immediately
func (t *Table) LoadBlockFromDir(dirname string, loadSpec *LoadSpec, load_records bool) TableBlock {
	tb := newTableBlock()
	tb.Name = dirname

	t.block_m.Lock()
	t.BlockList[dirname] = &tb
	t.block_m.Unlock()

	tb.table = t

	// find out how many records are kept in this dir...
	info := SavedColumnInfo{}
	istart := time.Now()
	filename := fmt.Sprintf("%s/info.db", dirname)
	file, _ := os.Open(filename)
	dec := gob.NewDecoder(file)
	dec.Decode(&info)
	iend := time.Now()

	if DEBUG_TIMING {
		log.Println("LOAD BLOCK INFO TOOK", iend.Sub(istart))
	}

	tb.allocateRecords(loadSpec, info, load_records)
	tb.Info = &info

	file, _ = os.Open(dirname)
	files, _ := file.Readdir(-1)

	for _, f := range files {
		fname := f.Name()

		if loadSpec != nil {
			if loadSpec.columns[fname] != true && load_records == false {
				continue
			}
		} else if load_records == false {
			continue
		}

		filename := fmt.Sprintf("%s/%s", dirname, fname)

		file, _ := os.Open(filename)
		dec := gob.NewDecoder(file)
		switch {
		case strings.HasPrefix(fname, "str"):
			tb.unpackStrCol(dec, info)

		case strings.HasPrefix(fname, "int"):
			tb.unpackIntCol(dec, info)
		}
	}

	return tb
}

// Remove our pointer to the blocklist so a GC is triggered and
// a bunch of new memory becomes available
func (t *Table) ReleaseRecords() {
	t.BlockList = make(map[string]*TableBlock, 0)
}

func (t *Table) LoadAndQueryRecords(loadSpec *LoadSpec, querySpec *QuerySpec) int {
	waystart := time.Now()
	log.Println("LOADING", t.Name)

	files, _ := ioutil.ReadDir(path.Join(*f_DIR, t.Name))

	var wg sync.WaitGroup
	block_specs := make(map[string]*QuerySpec)

	wg.Add(1)
	// why is table info so slow to open!!!
	go func() {
		defer wg.Done()
		t.LoadTableInfo()
	}()

	wg.Wait()

	m := &sync.Mutex{}

	count := 0
	for f := range files {
		v := files[len(files)-f-1]
		if strings.HasSuffix(v.Name(), "info.db") {
			continue
		}

		if strings.HasSuffix(v.Name(), "old") {
			continue
		}
		if strings.HasSuffix(v.Name(), "partial") {
			continue
		}

		if v.IsDir() {
			filename := path.Join(*f_DIR, t.Name, v.Name())
			wg.Add(1)
			load_all := false
			if loadSpec != nil && loadSpec.LoadAllColumns {
				load_all = true
			}

			go func() {
				defer wg.Done()
				start := time.Now()
				block := t.LoadBlockFromDir(filename, loadSpec, load_all)

				if *f_JSON == false {
					fmt.Print(".")
				}
				end := time.Now()
				if DEBUG_TIMING {
					if loadSpec != nil {
						log.Println("LOADED BLOCK FROM DIR", filename, "TOOK", end.Sub(start))
					} else {
						log.Println("LOADED INFO FOR BLOCK", filename, "TOOK", end.Sub(start))
					}
				}

				if len(block.RecordList) > 0 {
					m.Lock()
					count += len(block.RecordList)
					m.Unlock()

					if querySpec != nil {
						blockQuery := CopyQuerySpec(querySpec)

						ret := FilterAndAggRecords(blockQuery, &block.RecordList)
						blockQuery.Matched = ret

						m.Lock()
						block_specs[block.Name] = blockQuery
						delete(t.BlockList, block.Name)
						m.Unlock()
					}
				}
			}()

			if *f_SAMPLES {
				wg.Wait()

				if count > *f_LIMIT {
					break
				}
			}
		}

	}

	wg.Wait()

	if !*f_JSON {
		fmt.Print("\n")
	}
	// RE-POPULATE LOOKUP TABLE INFO
	t.populate_string_id_lookup()

	if f_LOAD_AND_QUERY != nil && *f_LOAD_AND_QUERY == true && querySpec != nil {
		// COMBINE THE PER BLOCK RESULTS
		astart := time.Now()
		resultSpec := CombineResults(block_specs)

		aend := time.Now()
		log.Println("AGGREGATING RESULT BLOCKS TOOK", aend.Sub(astart))

		querySpec.Results = resultSpec.Results
		querySpec.TimeResults = resultSpec.TimeResults

		SortResults(querySpec)
	}

	end := time.Now()

	if loadSpec != nil {
		log.Println(count, "RECORDS LOADED INTO", t.Name, "TOOK", end.Sub(waystart))
	} else {
		log.Println("INSPECTED", len(t.BlockList), "BLOCKS", "TOOK", end.Sub(waystart))
	}

	return count

}

func (t *Table) LoadRecords(loadSpec *LoadSpec) int {
	return t.LoadAndQueryRecords(loadSpec, nil)
}
