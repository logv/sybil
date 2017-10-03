package sybil

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/logv/sybil/src/lib/common"
	"github.com/logv/sybil/src/lib/config"

	. "github.com/logv/sybil/src/lib/aggregate"
	. "github.com/logv/sybil/src/lib/block_manager"
	. "github.com/logv/sybil/src/lib/encoders"
	. "github.com/logv/sybil/src/lib/filters"
	. "github.com/logv/sybil/src/lib/locks"
	. "github.com/logv/sybil/src/lib/metadata"
	. "github.com/logv/sybil/src/lib/specs"
	. "github.com/logv/sybil/src/lib/structs"
)

func SaveRecordsToBlock(t *Table, records RecordList, filename string) bool {
	if len(records) == 0 {
		return true
	}

	temp_block := NewTableBlock()
	temp_block.RecordList = records
	temp_block.Table = t

	return SaveToColumns(&temp_block, filename)
}

func FindPartialBlocks(t *Table) []*TableBlock {
	config.OPTS.READ_ROWS_ONLY = false
	LoadRecords(t, nil)

	ret := make([]*TableBlock, 0)

	t.BlockMutex.Lock()
	for _, v := range t.BlockList {
		if v.Name == ROW_STORE_BLOCK {
			continue
		}

		if v.Info.NumRecords < int32(CHUNK_SIZE) {
			ret = append(ret, v)
		}
	}
	t.BlockMutex.Unlock()

	return ret
}

// TODO: find any open blocks and then fill them...
func FillPartialBlock(t *Table) bool {
	if len(t.NewRecords) == 0 {
		return false
	}

	open_blocks := FindPartialBlocks(t)

	common.Debug("OPEN BLOCKS", open_blocks)
	var filename string

	if len(open_blocks) == 0 {
		return true
	}

	for _, b := range open_blocks {
		filename = b.Name
	}

	common.Debug("OPENING PARTIAL BLOCK", filename)

	if GrabBlockLock(t, filename) == false {
		common.Debug("CANT FILL PARTIAL BLOCK DUE TO LOCK", filename)
		return true
	}

	defer ReleaseBlockLock(t, filename)

	// open up our last record block, see how full it is
	delete(t.BlockInfoCache, filename)

	block := LoadBlockFromDir(t, filename, nil, true /* LOAD ALL RECORDS */)
	if block == nil {
		return true
	}

	partialRecords := block.RecordList
	common.Debug("LAST BLOCK HAS", len(partialRecords), "RECORDS")

	if len(partialRecords) < CHUNK_SIZE {
		delta := CHUNK_SIZE - len(partialRecords)
		if delta > len(t.NewRecords) {
			delta = len(t.NewRecords)
		}

		common.Debug("SAVING PARTIAL RECORDS", delta, "TO", filename)
		partialRecords = append(partialRecords, t.NewRecords[0:delta]...)
		if SaveRecordsToBlock(t, partialRecords, filename) == false {
			common.Debug("COULDNT SAVE PARTIAL RECORDS TO", filename)
			return false
		}

		if delta < len(t.NewRecords) {
			t.NewRecords = t.NewRecords[delta:]
		} else {
			t.NewRecords = make(RecordList, 0)
		}
	}

	return true
}

// optimizing for integer pre-cached info
func ShouldLoadBlockFromDir(t *Table, dirname string, querySpec *QuerySpec) bool {
	if querySpec == nil {
		return true
	}

	info := LoadBlockInfo(t, dirname)

	max_record := Record{Ints: IntArr{}, Strs: StrArr{}}
	min_record := Record{Ints: IntArr{}, Strs: StrArr{}}

	if len(info.IntInfoMap) == 0 {
		return true
	}

	for field_name, _ := range info.StrInfoMap {
		field_id := GetTableKeyID(t, field_name)
		ResizeFields(&min_record, field_id)
		ResizeFields(&max_record, field_id)
	}

	for field_name, field_info := range info.IntInfoMap {
		field_id := GetTableKeyID(t, field_name)
		ResizeFields(&min_record, field_id)
		ResizeFields(&max_record, field_id)

		min_record.Ints[field_id] = IntField(field_info.Min)
		max_record.Ints[field_id] = IntField(field_info.Max)

		min_record.Populated[field_id] = INT_VAL
		max_record.Populated[field_id] = INT_VAL
	}

	add := true
	for _, f := range querySpec.Filters {
		// make the minima record and the maxima records...
		switch fil := f.(type) {
		case IntFilter:
			if fil.Op == "gt" || fil.Op == "lt" {
				if f.Filter(&min_record) != true && f.Filter(&max_record) != true {
					add = false
					break
				}
			}
		}
	}

	return add
}

func LoadBlockInfo(t *Table, dirname string) *SavedColumnInfo {

	t.BlockMutex.Lock()
	cached_info, ok := t.BlockInfoCache[dirname]
	t.BlockMutex.Unlock()
	if ok {
		return cached_info
	}

	// find out how many records are kept in this dir...
	info := SavedColumnInfo{}
	istart := time.Now()
	filename := fmt.Sprintf("%s/info.db", dirname)

	err := DecodeInto(filename, &info)

	if err != nil {
		common.Warn("ERROR DECODING COLUMN BLOCK INFO!", dirname, err)
		return &info
	}
	iend := time.Now()

	if DEBUG_TIMING {
		common.Debug("LOAD BLOCK INFO TOOK", iend.Sub(istart))
	}

	t.BlockMutex.Lock()
	t.BlockInfoCache[dirname] = &info
	if info.NumRecords >= int32(CHUNK_SIZE) {
		t.NewBlockInfos = append(t.NewBlockInfos, dirname)
	}
	t.BlockMutex.Unlock()

	return &info
}

// TODO: have this only pull the blocks into column format and not materialize
// the columns immediately
func LoadBlockFromDir(t *Table, dirname string, loadSpec *LoadSpec, load_records bool) *TableBlock {
	tb := NewTableBlock()

	tb.Name = dirname

	tb.Table = t

	info := LoadBlockInfo(t, dirname)

	if info == nil {
		return nil
	}

	if info.NumRecords <= 0 {
		return nil
	}

	t.BlockMutex.Lock()
	t.BlockList[dirname] = &tb
	t.BlockMutex.Unlock()

	AllocateRecords(&tb, loadSpec, *info, load_records)
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

			if loadSpec.Files[cname] != true && load_records == false {
				continue
			}
		} else if load_records == false {
			continue
		}

		filename := fmt.Sprintf("%s/%s", dirname, fname)

		for _, ext := range []string{".pb", ".db", ".gb", ".gz"} {
			fname = strings.TrimRight(fname, ext)
		}

		dec := GetFileDecoder(filename)

		switch {
		case strings.HasPrefix(fname, "str") || strings.HasSuffix(fname, ".str"):
			unpackStrCol(&tb, dec, *info)
		case strings.HasPrefix(fname, "set") || strings.HasSuffix(fname, ".set"):
			unpackSetCol(&tb, dec, *info)
		case strings.HasPrefix(fname, "int") || strings.HasSuffix(fname, ".int"):
			unpackIntCol(&tb, dec, *info)
		}

		dec.CloseFile()

	}

	tb.Size = size

	file.Close()
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

	if *config.FLAGS.DEBUG {
		fmt.Fprint(os.Stderr, "+")
	}
}
