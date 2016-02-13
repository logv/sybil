package pcs

import "log"
import "fmt"
import "time"
import "os"
import "strings"
import "encoding/gob"
import "sync"
import "path"
import "strconv"

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

func (t *Table) SaveRecordsToBlock(records RecordList, filename string) {
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
			t.newRecords = make(RecordList, 0)
		}

	} else {
		incBlockId = true
	}

	if incBlockId {
		t.LastBlockId++
	}

	return true
}

// optimizing for integer pre-cached info
func (t *Table) ShouldLoadBlockFromDir(dirname string, querySpec *QuerySpec) bool {
	if querySpec == nil {
		return true
	}

	// find out how many records are kept in this dir...
	info := SavedColumnInfo{}
	filename := fmt.Sprintf("%s/info.db", dirname)
	file, _ := os.Open(filename)
	dec := gob.NewDecoder(file)
	dec.Decode(&info)

	max_record := Record{Ints: IntArr{}, Strs: StrArr{}}
	min_record := Record{Ints: IntArr{}, Strs: StrArr{}}

	if len(info.IntInfo) == 0 {
		return true
	}

	for field_id, _ := range info.StrInfo {
		min_record.ResizeFields(field_id)
		max_record.ResizeFields(field_id)
	}

	for field_id, field_info := range info.IntInfo {
		min_record.ResizeFields(field_id)
		max_record.ResizeFields(field_id)

		min_record.Ints[field_id] = IntField(field_info.Min)
		max_record.Ints[field_id] = IntField(field_info.Max)

		min_record.Populated[field_id] = INT_VAL
		max_record.Populated[field_id] = INT_VAL
	}

	add := true
	for _, f := range querySpec.Filters {
		// make the minima record and the maxima records...
		switch f.(type) {
		case IntFilter:
			if f.Filter(&min_record) != true && f.Filter(&max_record) != true {
				add = false
				break
			}
		}
	}

	return add
}

// TODO: have this only pull the blocks into column format and not materialize
// the columns immediately
func (t *Table) LoadBlockFromDir(dirname string, loadSpec *LoadSpec, load_records bool) *TableBlock {
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

	size := int64(0)

	for _, f := range files {
		fname := f.Name()
		fsize := f.Size()
		size += fsize

		if loadSpec != nil {
			if loadSpec.files[fname] != true && load_records == false {
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
		case strings.HasPrefix(fname, "set"):
			tb.unpackSetCol(dec, info)
		case strings.HasPrefix(fname, "int"):
			tb.unpackIntCol(dec, info)
		}
	}

	tb.Size = size

	return &tb
}

type AfterLoadQueryCB struct {
	querySpec *QuerySpec
	wg        *sync.WaitGroup
}

func (cb *AfterLoadQueryCB) CB(digestname string, records RecordList) {
	if digestname == NO_MORE_BLOCKS {
		cb.wg.Done()
		return
	}

	ret := FilterAndAggRecords(cb.querySpec, &records)

	if HOLD_MATCHES {
		log.Println("COPYING MATCHES")
		cb.querySpec.Matched = ret
	}

	fmt.Fprint(os.Stderr, "+")
}
