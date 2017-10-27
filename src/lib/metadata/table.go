package metadata

import . "github.com/logv/sybil/src/lib/common"
import . "github.com/logv/sybil/src/lib/structs"

func SetKeyType(t *Table, NameID int16, col_type int8) bool {
	cur_type, ok := t.KeyTypes[NameID]
	if !ok {
		t.KeyTypes[NameID] = col_type
	} else {
		if cur_type != col_type {
			Debug("TABLE", t.KeyTable)
			Debug("TYPES", t.KeyTypes)
			Warn("trying to over-write column type for key ", NameID, GetTableStringForKey(t, int(NameID)), " OLD TYPE", cur_type, " NEW TYPE", col_type)
			return false
		}
	}

	return true

}

func GetTableStringForKey(t *Table, id int) string {
	val, _ := t.KeyStringIDLookup[int16(id)]
	return val
}

func PopulateStringIDLookup(t *Table) {
	t.StringIDMutex.Lock()
	defer t.StringIDMutex.Unlock()

	t.KeyStringIDLookup = make(map[int16]string)
	t.ValStringIDLookup = make(map[int32]string)

	for k, v := range t.KeyTable {
		t.KeyStringIDLookup[v] = k
	}

	for _, b := range t.BlockList {
		if b.Columns == nil && b.Name != ROW_STORE_BLOCK {
			Debug("WARNING, BLOCK", b.Name, "IS SUSPECT! REMOVING FROM BLOCKLIST")
			t.BlockMutex.Lock()
			delete(t.BlockList, b.Name)
			t.BlockMutex.Unlock()
			continue
		}
		for _, c := range b.Columns {
			for k, v := range c.StringTable {
				c.ValStringIDLookup[v] = k
			}
		}

	}
}

func GetTableKeyID(t *Table, name string) int16 {
	t.StringIDMutex.RLock()
	id, ok := t.KeyTable[name]
	t.StringIDMutex.RUnlock()
	if ok {
		return int16(id)
	}

	t.StringIDMutex.Lock()
	defer t.StringIDMutex.Unlock()
	existing, ok := t.KeyTable[name]
	if ok {
		return existing
	}

	t.KeyTable[name] = int16(len(t.KeyTable))
	t.KeyStringIDLookup[t.KeyTable[name]] = name

	return int16(t.KeyTable[name])
}

func GetColumnType(t *Table, v string) int8 {
	col_id := GetTableKeyID(t, v)
	return t.KeyTypes[col_id]
}
