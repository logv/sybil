package sybil

type KeyInfo struct {
	Table    *Table
	KeyTypes map[int16]int8
	KeyTable map[string]int16
	IntInfo  IntInfoTable
	StrInfo  StrInfoTable

	KeyExchange map[int16]int16 // the key exchange maps the original table's keytable -> new key table
}

func (ki *KeyInfo) addKeys(keys []string) {
	for _, v := range keys {
		_, ok := ki.KeyTable[v]
		if ok {
			continue
		}

		Debug("ADDING KEY", v)

		key_id, ok := ki.Table.KeyTable[v]
		if !ok {
			continue
		}

		local_key_id := int16(len(ki.KeyTable))
		ki.KeyTypes[local_key_id] = ki.Table.KeyTypes[key_id]
		ki.KeyTable[v] = local_key_id
		ki.KeyExchange[key_id] = local_key_id

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
	ki.KeyExchange = make(map[int16]int16)
}

func (t *Table) UseKeys(keys []string) {
	if t.ShortKeyInfo == nil {
		t.ShortKeyInfo = &KeyInfo{}
		t.ShortKeyInfo.init_data_structures(t)

		t.AllKeyInfo = &KeyInfo{}
		t.AllKeyInfo.KeyTable = t.KeyTable
		t.AllKeyInfo.KeyTypes = t.KeyTypes
	}

	t.ShortKeyInfo.addKeys(keys)

}

func (t *Table) ShortenKeyTable() {
	Debug("TRIMMING KEY TABLE OF SIZE", len(t.KeyTable))
	if t.ShortKeyInfo == nil {
		Debug("NO KEY INFO WAS SETUP TO SHORTEN TABLE'S KEYS")
		return
	}

	t.KeyTypes = t.ShortKeyInfo.KeyTypes
	t.KeyTable = t.ShortKeyInfo.KeyTable
	t.IntInfo = t.ShortKeyInfo.IntInfo
	t.StrInfo = t.ShortKeyInfo.StrInfo
	Debug("NEW KEY TABLE", t.KeyTable)
	Debug("NEW KEY TYPES", t.KeyTypes)

}
