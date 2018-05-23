package sybil

import "sync"

type TableColumn struct {
	Type        int8
	StringTable map[string]int32
	RCache      map[int]bool

	block *TableBlock

	stringIdM         *sync.Mutex
	valStringIdLookup []string
}

func (tb *TableBlock) newTableColumn() *TableColumn {
	tc := TableColumn{}
	tc.StringTable = make(map[string]int32)
	tc.valStringIdLookup = make([]string, CHUNK_SIZE+1)
	tc.stringIdM = &sync.Mutex{}
	tc.block = tb
	tc.RCache = make(map[int]bool)

	return &tc
}

func (tc *TableColumn) getValId(name string) int32 {

	id, ok := tc.StringTable[name]

	if ok {
		return int32(id)
	}

	tc.stringIdM.Lock()
	tc.StringTable[name] = int32(len(tc.StringTable))

	// resize our string lookup if we need to
	if len(tc.StringTable) > len(tc.valStringIdLookup) {
		newLookup := make([]string, len(tc.StringTable)<<1)
		copy(newLookup, tc.valStringIdLookup)
		tc.valStringIdLookup = newLookup
	}

	tc.valStringIdLookup[tc.StringTable[name]] = name
	tc.stringIdM.Unlock()
	return tc.StringTable[name]
}

func (tc *TableColumn) getStringForVal(id int32) string {
	if int(id) >= len(tc.valStringIdLookup) {
		Warn("TRYING TO GET STRING ID FOR NON EXISTENT VAL", id)
		return ""
	}

	val := tc.valStringIdLookup[id]
	return val
}

func (tc *TableColumn) getStringForKey(id int) string {
	return tc.block.getStringForKey(int16(id))
}
