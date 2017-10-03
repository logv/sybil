package sybil

import (
	"strconv"

	"github.com/logv/sybil/src/lib/common"
	"github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/metadata"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/lib/table_column"
)

func BuildJoinMap(t *Table) {
	joinkey := *config.FLAGS.JOIN_KEY
	joinid := GetTableKeyID(t, joinkey)

	t.JoinLookup = make(map[string]*Record)

	common.Debug("BUILDING JOIN TABLE MAPPING")

	common.Debug("BLOCKS", len(t.BlockList))
	for _, b := range t.BlockList {
		for _, r := range b.RecordList {
			switch r.Populated[joinid] {
			case INT_VAL:
				val := strconv.FormatInt(int64(r.Ints[joinid]), 10)
				t.JoinLookup[val] = r

			case STR_VAL:
				col := GetColumnInfo(r.Block, joinid)
				t.JoinLookup[GetColumnStringForVal(col, int32(r.Strs[joinid]))] = r
			}

		}
	}

	common.Debug("ROWS", len(t.RowBlock.RecordList))
	for _, r := range t.RowBlock.RecordList {
		switch r.Populated[joinid] {
		case INT_VAL:
			val := strconv.FormatInt(int64(r.Ints[joinid]), 10)
			t.JoinLookup[val] = r

		case STR_VAL:
			col := GetColumnInfo(r.Block, joinid)
			t.JoinLookup[GetColumnStringForVal(col, int32(r.Strs[joinid]))] = r
		}

	}

}

func GetRecordById(t *Table, id string) *Record {

	return t.JoinLookup[id]
}
