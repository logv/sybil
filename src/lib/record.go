package pcs

type Record struct {
	Strs      []StrField
	Ints      []IntField
	SetMap    map[int16]SetField
	Populated []int8

	block *TableBlock
}

var INT_VAL = int8(1)
var STR_VAL = int8(2)
var SET_VAL = int8(3)

func (r *Record) GetStrVal(name string) (string, bool) {
	id := r.block.get_key_id(name)

	is := r.Strs[id]
	ok := r.Populated[id] == STR_VAL

	col := r.block.GetColumnInfo(id)
	val := col.get_string_for_val(int32(is))

	return val, ok
}

func (r *Record) GetIntVal(name string) (int, bool) {
	id := r.block.get_key_id(name)

	is := r.Ints[id]
	ok := r.Populated[id] == INT_VAL
	return int(is), ok
}

func (r *Record) GetSetVal(name string) ([]string, bool) {
	id := r.block.get_key_id(name)

	is := r.SetMap[id]
	ok := r.Populated[id] == SET_VAL

	col := r.block.GetColumnInfo(id)
	rets := make([]string, 0)

	if ok {
		for _, v := range is {
			val := col.get_string_for_val(int32(v))
			rets = append(rets, val)
		}
	}

	return rets, ok
}

func (r *Record) getVal(name string) (int, bool) {
	name_id := r.block.get_key_id(name)
	switch r.Populated[name_id] {
	case STR_VAL:
		return int(r.Strs[name_id]), true

	case INT_VAL:
		return int(r.Ints[name_id]), true

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
		delta_records := make([]StrField, int(float64(length)))

		r.Strs = append(r.Strs, delta_records...)
	}

	if int(length) >= len(r.Populated) {
		delta_records := make([]int8, int(float64(length)))

		r.Populated = append(r.Populated, delta_records...)
	}

	if int(length) >= len(r.Ints) {
		delta_records := make([]IntField, int(float64(length)))

		r.Ints = append(r.Ints, delta_records...)
	}

}

func (r *Record) AddStrField(name string, val string) {
	name_id := r.block.get_key_id(name)

	col := r.block.GetColumnInfo(name_id)
	value_id := col.get_val_id(val)

	r.ResizeFields(name_id)
	r.Strs[name_id] = StrField(value_id)
	r.Populated[name_id] = STR_VAL

	r.block.table.set_key_type(name_id, STR_VAL)
}

func (r *Record) AddIntField(name string, val int64) {
	name_id := r.block.get_key_id(name)
	r.block.table.update_int_info(name_id, val)

	r.ResizeFields(name_id)
	r.Ints[name_id] = IntField(val)
	r.Populated[name_id] = INT_VAL
	r.block.table.set_key_type(name_id, INT_VAL)
}

func (r *Record) AddSetField(name string, val []string) {
	name_id := r.block.get_key_id(name)
	vals := make([]int32, len(val))
	for i, v := range val {
		col := r.block.GetColumnInfo(name_id)
		vals[i] = col.get_val_id(v)
	}

	r.ResizeFields(name_id)
	if r.SetMap == nil {
		r.SetMap = make(map[int16]SetField)
	}

	r.SetMap[name_id] = SetField(vals)
	r.Populated[name_id] = SET_VAL
	r.block.table.set_key_type(name_id, SET_VAL)
}
