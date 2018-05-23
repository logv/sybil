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
	id := r.block.getKeyId(name)

	is := r.Strs[id]
	ok := r.Populated[id] == STR_VAL

	col := r.block.GetColumnInfo(id)
	val := col.getStringForVal(int32(is))

	return val, ok
}

func (r *Record) GetIntVal(name string) (int, bool) {
	id := r.block.getKeyId(name)

	is := r.Ints[id]
	ok := r.Populated[id] == INT_VAL
	return int(is), ok
}

func (r *Record) GetSetVal(name string) ([]string, bool) {
	id := r.block.getKeyId(name)

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
	nameId := r.block.getKeyId(name)
	switch r.Populated[nameId] {
	case STR_VAL:
		return int(r.Strs[nameId]), true

	case INT_VAL:
		return int(r.Ints[nameId]), true

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
	nameId := r.block.getKeyId(name)

	col := r.block.GetColumnInfo(nameId)
	valueId := col.getValId(val)

	r.ResizeFields(nameId)
	r.Strs[nameId] = StrField(valueId)
	r.Populated[nameId] = STR_VAL

	if r.block.table.setKeyType(nameId, STR_VAL) == false {
		Error("COULDNT SET STR VAL", name, val, nameId)
	}
}

func (r *Record) AddIntField(name string, val int64) {
	nameId := r.block.getKeyId(name)
	r.block.table.updateIntInfo(nameId, val)

	r.ResizeFields(nameId)
	r.Ints[nameId] = IntField(val)
	r.Populated[nameId] = INT_VAL
	if r.block.table.setKeyType(nameId, INT_VAL) == false {
		Error("COULDNT SET INT VAL", name, val, nameId)
	}
}

func (r *Record) AddSetField(name string, val []string) {
	nameId := r.block.getKeyId(name)
	vals := make([]int32, len(val))
	for i, v := range val {
		col := r.block.GetColumnInfo(nameId)
		vals[i] = col.getValId(v)
	}

	r.ResizeFields(nameId)
	if r.SetMap == nil {
		r.SetMap = make(map[int16]SetField)
	}

	r.SetMap[nameId] = SetField(vals)
	r.Populated[nameId] = SET_VAL
	if r.block.table.setKeyType(nameId, SET_VAL) == false {
		Error("COULDNT SET SET VAL", name, val, nameId)
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
		for i, _ := range r.Populated {
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
