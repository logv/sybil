package sybil

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	md "github.com/logv/sybil/src/lib/metadata"
	. "github.com/logv/sybil/src/lib/structs"
	col_store "github.com/logv/sybil/src/storage/column_store"
	encoders "github.com/logv/sybil/src/storage/encoders"
)

// TODO: have this only pull the blocks into column format and not materialize
// the columns immediately
func ReadBlockInfoFromDir(t *Table, dirname string) *SavedColumnInfo {
	tb := NewTableBlock()

	tb.Name = dirname

	tb.Table = t

	// find out how many records are kept in this dir...
	info := SavedColumnInfo{}
	filename := fmt.Sprintf("%s/info.db", dirname)

	err := encoders.DecodeInto(filename, &info)

	if err != nil {
		Warn("ERROR DECODING COLUMN BLOCK INFO!", dirname, err)
		return nil
	}

	if info.NumRecords <= 0 {
		return nil
	}

	file, _ := os.Open(dirname)
	files, _ := file.Readdir(-1)

	size := int64(0)

	var wg sync.WaitGroup
	columns := make(map[string]int)

	for _, f := range files {
		fname := f.Name()
		fsize := f.Size()
		size += fsize
		col_name := fname
		col_type := NO_VAL

		col_name = strings.TrimRight(col_name, ".gz")
		col_name = strings.TrimRight(col_name, ".db")

		switch {
		case strings.HasPrefix(fname, "str"):
			col_name = strings.Replace(col_name, "str_", "", 1)
			col_type = STR_VAL
		case strings.HasPrefix(col_name, "set"):
			col_name = strings.Replace(col_name, "set_", "", 1)
			col_type = SET_VAL
		case strings.HasPrefix(col_name, "int"):
			col_name = strings.Replace(col_name, "int_", "", 1)
			col_type = INT_VAL

			col_info := info.IntInfoMap[col_name]
			col_id := md.GetTableKeyID(t, col_name)
			int_info, ok := t.IntInfo[col_id]
			if !ok {
				t.IntInfo[col_id] = col_info
			} else {
				if col_info.Min < int_info.Min {
					int_info.Min = col_info.Min
				}
			}
		}

		if col_name != "" {
			col_id := md.GetTableKeyID(t, col_name)
			md.SetKeyType(t, col_id, int8(col_type))
			columns[col_name] = int(col_type)
		}

	}

	wg.Wait()

	return &info
}

// Alright, so... I accidentally broke my info.db file
// How can I go about loading the TableInfo based off the blocks?
// I think I go through each block and load the block, verifying the different
// column types
func DeduceTableInfoFromBlocks(t *Table) {
	files, _ := ioutil.ReadDir(path.Join(*FLAGS.DIR, t.Name))

	var wg sync.WaitGroup
	InitDataStructures(t)

	saved_table := Table{}
	saved_table.Name = t.Name

	InitDataStructures(&saved_table)

	this_block := 0
	m := &sync.Mutex{}

	type_counts := make(map[string]map[int8]int)

	broken_mutex := sync.Mutex{}
	broken_blocks := make([]string, 0)
	for f := range files {

		v := files[f]
		if v.IsDir() && col_store.FileLooksLikeBlock(v) {
			filename := path.Join(*FLAGS.DIR, t.Name, v.Name())
			this_block++

			wg.Add(1)
			go func() {
				defer wg.Done()

				info := ReadBlockInfoFromDir(t, filename)
				if info == nil {
					broken_mutex.Lock()
					broken_blocks = append(broken_blocks, filename)
					broken_mutex.Unlock()
					return
				}

				m.Lock()
				for col := range info.IntInfoMap {
					_, ok := type_counts[col]
					if !ok {
						type_counts[col] = make(map[int8]int)
					}
					type_counts[col][INT_VAL]++
				}
				for col := range info.StrInfoMap {
					_, ok := type_counts[col]
					if !ok {
						type_counts[col] = make(map[int8]int)
					}
					type_counts[col][STR_VAL]++
				}
				m.Unlock()

			}()
		}
	}

	wg.Wait()

	// TODO: verify that the KEY TABLE and KEY TYPES
	Debug("TYPE COUNTS", this_block, type_counts)
	Debug("KEY TABLE", t.KeyTable)
	Debug("KEY TYPES", t.KeyTypes)

}
