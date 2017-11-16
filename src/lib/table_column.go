package sybil

import "sync"

type TableColumn struct {
	Type        int8
	StringTable map[string]int32
	RCache      map[int]bool

	block *TableBlock

	string_id_m          *sync.Mutex
	val_string_id_lookup []string
}

func (tb *TableBlock) newTableColumn() *TableColumn {
	tc := TableColumn{}
	tc.StringTable = make(map[string]int32)
	tc.val_string_id_lookup = make([]string, CHUNK_SIZE+1)
	tc.string_id_m = &sync.Mutex{}
	tc.block = tb
	tc.RCache = make(map[int]bool)

	return &tc
}

func (tc *TableColumn) get_val_id(name string) int32 {

	id, ok := tc.StringTable[name]

	if ok {
		return int32(id)
	}

	tc.string_id_m.Lock()
	tc.StringTable[name] = int32(len(tc.StringTable))

	// resize our string lookup if we need to
	if len(tc.StringTable) > len(tc.val_string_id_lookup) {
		new_lookup := make([]string, len(tc.StringTable)<<1)
		copy(new_lookup, tc.val_string_id_lookup)
		tc.val_string_id_lookup = new_lookup
	}

	tc.val_string_id_lookup[tc.StringTable[name]] = name
	tc.string_id_m.Unlock()
	return tc.StringTable[name]
}

func (tc *TableColumn) get_string_for_val(id int32) string {
	if int(id) >= len(tc.val_string_id_lookup) {
		Warn("TRYING TO GET STRING ID FOR NON EXISTENT VAL", id)
		return ""
	}

	val := tc.val_string_id_lookup[id]
	return val
}

func (tc *TableColumn) get_string_for_key(id int) string {
	return tc.block.get_string_for_key(int16(id))
}
