package sybil

import "strconv"

func (t *Table) BuildJoinMap() {
	joinkey := *FLAGS.JoinKey
	joinid := t.getKeyID(joinkey)

	t.joinLookup = make(map[string]*Record)

	Debug("BUILDING JOIN TABLE MAPPING")

	Debug("BLOCKS", len(t.BlockList))
	for _, b := range t.BlockList {
		for _, r := range b.RecordList {
			switch r.Populated[joinid] {
			case IntVal:
				val := strconv.FormatInt(int64(r.Ints[joinid]), 10)
				t.joinLookup[val] = r

			case StrVal:
				col := r.block.GetColumnInfo(joinid)
				t.joinLookup[col.getStringForVal(int32(r.Strs[joinid]))] = r
			}

		}
	}

	Debug("ROWS", len(t.RowBlock.RecordList))
	for _, r := range t.RowBlock.RecordList {
		switch r.Populated[joinid] {
		case IntVal:
			val := strconv.FormatInt(int64(r.Ints[joinid]), 10)
			t.joinLookup[val] = r

		case StrVal:
			col := r.block.GetColumnInfo(joinid)
			t.joinLookup[col.getStringForVal(int32(r.Strs[joinid]))] = r
		}

	}

}

func (t *Table) GetRecordByID(id string) *Record {

	return t.joinLookup[id]
}
