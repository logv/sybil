package sybil

import "sync"

type TableColumn struct {
	Type        int8
	StringTable map[string]int32
	RCache      map[int]bool

	block *TableBlock

	stringIDMutex     *sync.Mutex
	valStringIDLookup map[int32]string
}

func (tb *TableBlock) newTableColumn() *TableColumn {
	tc := TableColumn{}
	tc.StringTable = make(map[string]int32)
	tc.valStringIDLookup = make(map[int32]string)
	tc.stringIDMutex = &sync.Mutex{}
	tc.block = tb
	tc.RCache = make(map[int]bool)

	return &tc
}

func (tc *TableColumn) getValID(name string) int32 {

	id, ok := tc.StringTable[name]

	if ok {
		return int32(id)
	}

	tc.stringIDMutex.Lock()
	tc.StringTable[name] = int32(len(tc.StringTable))
	tc.valStringIDLookup[tc.StringTable[name]] = name
	tc.stringIDMutex.Unlock()
	return tc.StringTable[name]
}

func (tc *TableColumn) getStringForVal(id int32) string {
	val, _ := tc.valStringIDLookup[id]
	return val
}

func (tc *TableColumn) getStringForKey(id int) string {
	return tc.block.getStringForKey(int16(id))
}
