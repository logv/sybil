package sybil

type Record struct {
	Strs      []StrField
	Ints      []IntField
	SetMap    map[int16]SetField
	Populated []int8

	Timestamp int64
	Path      string

	block *TableBlock
}

const (
	_NO_VAL = iota
	INT_VAL = iota
	STR_VAL = iota
	SET_VAL = iota
)

func (r *Record) GetStrVal(name string) (string, bool) {
	id := r.block.getKeyID(name)

	is := r.Strs[id]
	ok := r.Populated[id] == STR_VAL

	col := r.block.GetColumnInfo(id)
	val := col.getStringForVal(int32(is))

	return val, ok
}

func (r *Record) GetIntVal(name string) (int, bool) {
	id := r.block.getKeyID(name)

	is := r.Ints[id]
	ok := r.Populated[id] == INT_VAL
	return int(is), ok
}

func (r *Record) GetSetVal(name string) ([]string, bool) {
	id := r.block.getKeyID(name)

	is := r.SetMap[id]
	ok := r.Populated[id] == SET_VAL

	col := r.block.GetColumnInfo(id)
	rets := make([]string, 0)

	if ok {
		for _, v := range is {
			val := col.getStringForVal(int32(v))
			rets = append(rets, val)
		}
	}

	return rets, ok
}

func (r *Record) getVal(name string) (int, bool) {
	nameID := r.block.getKeyID(name)
	switch r.Populated[nameID] {
	case STR_VAL:
		return int(r.Strs[nameID]), true

	case INT_VAL:
		return int(r.Ints[nameID]), true

	default:
		return 0, false
	}

}

func (r *Record) ResizeFields(length int16) {
	// dont get fooled by zeroes
	if length <= 1 {
		length = 5
	}

	length++

	if int(length) >= len(r.Strs) {
		deltaRecords := make([]StrField, int(float64(length)))

		r.Strs = append(r.Strs, deltaRecords...)
	}

	if int(length) >= len(r.Populated) {
		deltaRecords := make([]int8, int(float64(length)))

		r.Populated = append(r.Populated, deltaRecords...)
	}

	if int(length) >= len(r.Ints) {
		deltaRecords := make([]IntField, int(float64(length)))

		r.Ints = append(r.Ints, deltaRecords...)
	}

}

func (r *Record) AddStrField(name string, val string) {
	nameID := r.block.getKeyID(name)

	col := r.block.GetColumnInfo(nameID)
	valueID := col.getValID(val)

	r.ResizeFields(nameID)
	r.Strs[nameID] = StrField(valueID)
	r.Populated[nameID] = STR_VAL

	if r.block.table.setKeyType(nameID, STR_VAL) == false {
		Error("COULDNT SET STR VAL", name, val, nameID)
	}
}

func (r *Record) AddIntField(name string, val int64) {
	nameID := r.block.getKeyID(name)
	r.block.table.updateIntInfo(nameID, val)

	r.ResizeFields(nameID)
	r.Ints[nameID] = IntField(val)
	r.Populated[nameID] = INT_VAL
	if r.block.table.setKeyType(nameID, INT_VAL) == false {
		Error("COULDNT SET INT VAL", name, val, nameID)
	}
}

func (r *Record) AddSetField(name string, val []string) {
	nameID := r.block.getKeyID(name)
	vals := make([]int32, len(val))
	for i, v := range val {
		col := r.block.GetColumnInfo(nameID)
		vals[i] = col.getValID(v)
	}

	r.ResizeFields(nameID)
	if r.SetMap == nil {
		r.SetMap = make(map[int16]SetField)
	}

	r.SetMap[nameID] = SetField(vals)
	r.Populated[nameID] = SET_VAL
	if r.block.table.setKeyType(nameID, SET_VAL) == false {
		Error("COULDNT SET SET VAL", name, val, nameID)
	}
}

var COPY_RECORD_INTERNS = false

func (r *Record) CopyRecord() *Record {
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

	nr.block = r.block

	return &nr
}
