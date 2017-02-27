package sybil

import "fmt"
import "time"
import "os"
import "strings"
import "sync"

var GZIP_EXT = ".gz"

func (t *Table) SaveRecordsToBlock(records RecordList, filename string) {
	if len(records) == 0 {
		return
	}

	temp_block := newTableBlock()
	temp_block.RecordList = records
	temp_block.table = t

	temp_block.SaveToColumns(filename)
}

func (t *Table) FindPartialBlocks() []*TableBlock {
	READ_ROWS_ONLY = false
	t.LoadRecords(nil)

	ret := make([]*TableBlock, 0)

	t.block_m.Lock()
	for _, v := range t.BlockList {
		if v.Name == ROW_STORE_BLOCK {
			continue
		}

		if v.Info.NumRecords < int32(CHUNK_SIZE) {
			ret = append(ret, v)
		}
	}
	t.block_m.Unlock()

	return ret
}

// TODO: find any open blocks and then fill them...
func (t *Table) FillPartialBlock() bool {
	if len(t.newRecords) == 0 {
		return false
	}

	open_blocks := t.FindPartialBlocks()

	Debug("OPEN BLOCKS", open_blocks)
	var filename string

	if len(open_blocks) == 0 {
		return true
	}

	for _, b := range open_blocks {
		filename = b.Name
	}

	Debug("OPENING PARTIAL BLOCK", filename)

	if t.GrabBlockLock(filename) == false {
		Debug("CANT FILL PARTIAL BLOCK DUE TO LOCK", filename)
		return true
	}

	defer t.ReleaseBlockLock(filename)

	// Open up our last record block, see how full it is
	block := t.LoadBlockFromDir(filename, nil, true /* LOAD ALL RECORDS */)
	if block == nil {
		return true
	}

	partialRecords := block.RecordList
	Debug("LAST BLOCK HAS", len(partialRecords), "RECORDS")

	if len(partialRecords) < CHUNK_SIZE {
		delta := CHUNK_SIZE - len(partialRecords)
		if delta > len(t.newRecords) {
			delta = len(t.newRecords)
		}

		Debug("SAVING PARTIAL RECORDS", delta, "TO", filename)
		partialRecords = append(partialRecords, t.newRecords[0:delta]...)
		t.SaveRecordsToBlock(partialRecords, filename)
		if delta < len(t.newRecords) {
			t.newRecords = t.newRecords[delta:]
		} else {
			t.newRecords = make(RecordList, 0)
		}
	}

	return true
}

// optimizing for integer pre-cached info
func (t *Table) ShouldLoadBlockFromDir(dirname string, querySpec *QuerySpec) bool {
	if querySpec == nil {
		return true
	}

	info := t.LoadBlockInfo(dirname)

	max_record := Record{Ints: IntArr{}, Strs: StrArr{}}
	min_record := Record{Ints: IntArr{}, Strs: StrArr{}}

	if len(info.IntInfoMap) == 0 {
		return true
	}

	for field_name, _ := range info.StrInfoMap {
		field_id := t.get_key_id(field_name)
		min_record.ResizeFields(field_id)
		max_record.ResizeFields(field_id)
	}

	for field_name, field_info := range info.IntInfoMap {
		field_id := t.get_key_id(field_name)
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

func (t *Table) LoadBlockInfo(dirname string) *SavedColumnInfo {

	t.block_m.Lock()
	cached_info, ok := t.BlockInfoCache[dirname]
	t.block_m.Unlock()
	if ok {
		return cached_info
	}

	// find out how many records are kept in this dir...
	info := SavedColumnInfo{}
	istart := time.Now()
	filename := fmt.Sprintf("%s/info.db", dirname)

	dec := GetFileDecoder(filename)
	err := dec.Decode(&info)

	if err != nil {
		Warn("ERROR DECODING COLUMN BLOCK INFO!", dirname, err)
		return &info
	}
	iend := time.Now()

	if DEBUG_TIMING {
		Debug("LOAD BLOCK INFO TOOK", iend.Sub(istart))
	}

	t.block_m.Lock()
	t.BlockInfoCache[dirname] = &info
	if info.NumRecords >= int32(CHUNK_SIZE) {
		t.NewBlockInfos = append(t.NewBlockInfos, dirname)
	}
	t.block_m.Unlock()

	return &info
}

// TODO: have this only pull the blocks into column format and not materialize
// the columns immediately
func (t *Table) LoadBlockFromDir(dirname string, loadSpec *LoadSpec, load_records bool) *TableBlock {
	tb := newTableBlock()

	tb.Name = dirname

	tb.table = t

	info := t.LoadBlockInfo(dirname)

	if info == nil {
		return nil
	}

	if info.NumRecords <= 0 {
		return nil
	}

	t.block_m.Lock()
	t.BlockList[dirname] = &tb
	t.block_m.Unlock()

	tb.allocateRecords(loadSpec, *info, load_records)
	tb.Info = info

	file, _ := os.Open(dirname)
	files, _ := file.Readdir(-1)

	size := int64(0)

	for _, f := range files {
		fname := f.Name()
		fsize := f.Size()
		size += fsize

		// over here, we have to accomodate .gz extension, i guess
		if loadSpec != nil {
			// we cut off extensions to check our loadSpec
			cname := strings.TrimRight(fname, GZIP_EXT)
			if loadSpec.files[cname] != true && load_records == false {
				continue
			}
		} else if load_records == false {
			continue
		}

		filename := fmt.Sprintf("%s/%s", dirname, fname)

		dec := GetFileDecoder(filename)

		switch {
		case strings.HasPrefix(fname, "str"):
			tb.unpackStrCol(dec, *info)
		case strings.HasPrefix(fname, "set"):
			tb.unpackSetCol(dec, *info)
		case strings.HasPrefix(fname, "int"):
			tb.unpackIntCol(dec, *info)
		}
	}

	tb.Size = size

	return &tb
}

type AfterLoadQueryCB struct {
	querySpec *QuerySpec
	wg        *sync.WaitGroup
	records   RecordList

	count int
}

func (cb *AfterLoadQueryCB) CB(digestname string, records RecordList) {
	if digestname == NO_MORE_BLOCKS {
		// TODO: add sessionization call over here, too
		count := FilterAndAggRecords(cb.querySpec, &cb.records)
		cb.count += count

		cb.wg.Done()
		return
	}

	querySpec := cb.querySpec

	for _, r := range records {
		add := true
		// FILTERING
		for j := 0; j < len(querySpec.Filters); j++ {
			// returns True if the record matches!
			ret := querySpec.Filters[j].Filter(r) != true
			if ret {
				add = false
				break
			}
		}

		if !add {
			continue
		}

		cb.records = append(cb.records, r)
	}

	if *DEBUG_FLAG {
		fmt.Fprint(os.Stderr, "+")
	}
}
