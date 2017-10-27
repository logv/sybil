package sybil

import (
	"fmt"
	"os"
	"strings"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"

	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/query/slab_manager"
	. "github.com/logv/sybil/src/query/specs"
	. "github.com/logv/sybil/src/storage/encoders"
	. "github.com/logv/sybil/src/storage/file_locks"
	. "github.com/logv/sybil/src/storage/metadata_io"
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
	OPTS.READ_ROWS_ONLY = false
	LOAD_RECORDS_FUNC(t, nil)

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

	Debug("OPEN BLOCKS", open_blocks)
	var filename string

	if len(open_blocks) == 0 {
		return true
	}

	for _, b := range open_blocks {
		filename = b.Name
	}

	Debug("OPENING PARTIAL BLOCK", filename)

	if GrabBlockLock(t, filename) == false {
		Debug("CANT FILL PARTIAL BLOCK DUE TO LOCK", filename)
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
	Debug("LAST BLOCK HAS", len(partialRecords), "RECORDS")

	if len(partialRecords) < CHUNK_SIZE {
		delta := CHUNK_SIZE - len(partialRecords)
		if delta > len(t.NewRecords) {
			delta = len(t.NewRecords)
		}

		Debug("SAVING PARTIAL RECORDS", delta, "TO", filename)
		partialRecords = append(partialRecords, t.NewRecords[0:delta]...)
		if SaveRecordsToBlock(t, partialRecords, filename) == false {
			Debug("COULDNT SAVE PARTIAL RECORDS TO", filename)
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

		for _, ext := range []string{".pb", ".db", ".gob", ".gz"} {
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
