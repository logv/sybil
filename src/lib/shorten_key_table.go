package sybil

type KeyInfo struct {
	Table    *Table
	KeyTypes map[int16]int8
	KeyTable map[string]int16
	IntInfo  IntInfoTable
	StrInfo  StrInfoTable
}

func (ki *KeyInfo) addKeys(keys []string) {
	for _, v := range keys {
		_, ok := ki.KeyTable[v]
		if ok {
			continue
		}

		Debug("ADDING KEY", v)

		key_id := ki.Table.KeyTable[v]
		local_key_id := int16(len(ki.KeyTable))
		ki.KeyTypes[local_key_id] = ki.Table.KeyTypes[key_id]
		ki.KeyTable[v] = local_key_id

		int_info, ok := ki.Table.IntInfo[key_id]
		if ok {
			ki.IntInfo[local_key_id] = int_info
		}

		str_info, ok := ki.Table.StrInfo[key_id]
		if ok {
			ki.StrInfo[local_key_id] = str_info
		}

	}
}

func (ki *KeyInfo) init_data_structures(t *Table) {
	ki.Table = t
	ki.KeyTypes = make(map[int16]int8)
	ki.KeyTable = make(map[string]int16)
	ki.IntInfo = make(IntInfoTable)
	ki.StrInfo = make(StrInfoTable)
}

func (t *Table) UseKeys(keys []string) {
	if t.KeyInfo == nil {
		t.KeyInfo = &KeyInfo{}
		t.KeyInfo.init_data_structures(t)
	}

	t.KeyInfo.addKeys(keys)

}

func (t *Table) ShortenKeyTable() {
	Debug("TRIMMING KEY TABLE OF SIZE", len(t.KeyTable))
	if t.KeyInfo == nil {
		Debug("NO KEY INFO WAS SETUP TO SHORTEN TABLE'S KEYS")
		return
	}
	t.KeyTypes = t.KeyInfo.KeyTypes
	t.KeyTable = t.KeyInfo.KeyTable
	t.IntInfo = t.KeyInfo.IntInfo
	t.StrInfo = t.KeyInfo.StrInfo
	Debug("NEW KEY TABLE", t.KeyTable)
	Debug("NEW KEY TYPES", t.KeyTypes)

}
