package sybil


import "strconv"

func (t *Table) BuildJoinMap() {
	joinkey := *FLAGS.JOIN_KEY
	joinid := t.get_key_id(joinkey)

	t.join_lookup = make(map[string]*Record)

	Debug("BUILDING JOIN TABLE MAPPING")

	Debug("BLOCKS", len(t.BlockList))
	for _, b := range t.BlockList {
		for _, r := range b.RecordList {
			switch r.Populated[joinid] {
			case INT_VAL:
				val := strconv.FormatInt(int64(r.Ints[joinid]), 10)
				t.join_lookup[val] = r

			case STR_VAL:
				col := r.block.GetColumnInfo(joinid)
				t.join_lookup[col.get_string_for_val(int32(r.Strs[joinid]))] = r
			}

		}
	}

	Debug("ROWS", len(t.RowBlock.RecordList))
	for _, r := range t.RowBlock.RecordList {
		switch r.Populated[joinid] {
		case INT_VAL:
			val := strconv.FormatInt(int64(r.Ints[joinid]), 10)
			t.join_lookup[val] = r

		case STR_VAL:
			col := r.block.GetColumnInfo(joinid)
			t.join_lookup[col.get_string_for_val(int32(r.Strs[joinid]))] = r
		}

	}

}

func (t *Table) GetRecordById(id string) *Record {

	return t.join_lookup[id]
}
