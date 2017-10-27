package sybil

import . "github.com/logv/sybil/src/lib/common"
import . "github.com/logv/sybil/src/lib/structs"
import . "github.com/logv/sybil/src/lib/metadata"

func GetStrVal(r *Record, name string) (string, bool) {
	id := GetBlockKeyID(r.Block, name)

	is := r.Strs[id]
	ok := r.Populated[id] == STR_VAL

	col := GetColumnInfo(r.Block, id)
	val := GetColumnStringForVal(col, int32(is))

	return val, ok
}

func GetIntVal(r *Record, name string) (int, bool) {
	id := GetBlockKeyID(r.Block, name)

	is := r.Ints[id]
	ok := r.Populated[id] == INT_VAL
	return int(is), ok
}

func GetSetVal(r *Record, name string) ([]string, bool) {
	id := GetBlockKeyID(r.Block, name)

	is := r.SetMap[id]
	ok := r.Populated[id] == SET_VAL

	col := GetColumnInfo(r.Block, id)
	rets := make([]string, 0)

	if ok {
		for _, v := range is {
			val := GetColumnStringForVal(col, int32(v))
			rets = append(rets, val)
		}
	}

	return rets, ok
}

func AddStrField(r *Record, name string, val string) {
	NameID := GetBlockKeyID(r.Block, name)

	col := GetColumnInfo(r.Block, NameID)
	value_id := GetColumnValID(col, val)

	ResizeFields(r, NameID)
	r.Strs[NameID] = StrField(value_id)
	r.Populated[NameID] = STR_VAL

	if SetKeyType(r.Block.Table, NameID, STR_VAL) == false {
		Error("COULDNT SET STR VAL", name, val, NameID)
	}
}

func AddIntField(r *Record, name string, val int64) {
	NameID := GetBlockKeyID(r.Block, name)
	UpdateBlockIntInfo(r.Block, NameID, val)

	ResizeFields(r, NameID)
	r.Ints[NameID] = IntField(val)
	r.Populated[NameID] = INT_VAL
	if SetKeyType(r.Block.Table, NameID, INT_VAL) == false {
		Error("COULDNT SET INT VAL", name, val, NameID)
	}
}

func AddSetField(r *Record, name string, val []string) {
	NameID := GetBlockKeyID(r.Block, name)
	vals := make([]int32, len(val))
	for i, v := range val {
		col := GetColumnInfo(r.Block, NameID)
		vals[i] = GetColumnValID(col, v)
	}

	ResizeFields(r, NameID)
	if r.SetMap == nil {
		r.SetMap = make(map[int16]SetField)
	}

	r.SetMap[NameID] = SetField(vals)
	r.Populated[NameID] = SET_VAL
	if SetKeyType(r.Block.Table, NameID, SET_VAL) == false {
		Error("COULDNT SET SET VAL", name, val, NameID)
	}
}

var COPY_RECORD_INTERNS = false

func CopyRecord(r *Record) *Record {
	nr := Record{}

	if len(r.Ints) > 0 {
		if COPY_RECORD_INTERNS {
			nr.Ints = r.Ints
		} else {
			nr.Ints = make([]IntField, len(r.Populated))
		}
	}

	if len(r.Strs) > 0 {
		if COPY_RECORD_INTERNS {
			nr.Strs = r.Strs
		} else {
			nr.Strs = make([]StrField, len(r.Populated))
		}
	}

	if len(r.SetMap) > 0 {
		nr.SetMap = r.SetMap
	}

	if COPY_RECORD_INTERNS {
		nr.Populated = r.Populated
	} else {
		nr.Populated = make([]int8, len(r.Populated))
		for i := range r.Populated {
			nr.Strs[i] = r.Strs[i]
			nr.Ints[i] = r.Ints[i]
			nr.Populated[i] = r.Populated[i]
		}
	}

	nr.Timestamp = r.Timestamp
	nr.Path = r.Path

	nr.Block = r.Block

	return &nr
}

func NewRecord(t *Table) *Record {
	r := Record{Ints: IntArr{}, Strs: StrArr{}}

	b := t.LastBlock
	b.Table = t
	r.Block = &b

	t.RecordMutex.Lock()
	t.NewRecords = append(t.NewRecords, &r)
	t.RecordMutex.Unlock()
	return &r
}
