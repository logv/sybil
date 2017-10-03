package table_column

import "sync"

import . "github.com/logv/sybil/src/lib/structs"
import "github.com/logv/sybil/src/lib/metadata"

func NewTableColumn(tb *TableBlock) *TableColumn {
	tc := TableColumn{}
	tc.StringTable = make(map[string]int32)
	tc.ValStringIDLookup = make(map[int32]string)
	tc.StringIDMutex = &sync.Mutex{}
	tc.Block = tb
	tc.RCache = make(map[int]bool)

	return &tc
}

func GetColumnValID(tc *TableColumn, name string) int32 {

	id, ok := tc.StringTable[name]

	if ok {
		return int32(id)
	}

	tc.StringIDMutex.Lock()
	tc.StringTable[name] = int32(len(tc.StringTable))
	tc.ValStringIDLookup[tc.StringTable[name]] = name
	tc.StringIDMutex.Unlock()
	return tc.StringTable[name]
}

func GetColumnStringForVal(tc *TableColumn, id int32) string {
	val, _ := tc.ValStringIDLookup[id]
	return val
}

func GetColumnStringForKey(tc *TableColumn, id int) string {
	return metadata.GetBlockStringForKey(tc.Block, int16(id))
}

func GetColumnInfo(tb *TableBlock, NameID int16) *TableColumn {
	col, ok := tb.Columns[NameID]
	if !ok {
		col = NewTableColumn(tb)
		tb.Columns[NameID] = col
	}

	return col
}
