package sybil


import "sync"

type LoadSpec struct {
	columns map[string]bool
	files   map[string]bool

	LoadAllColumns bool
	table          *Table

	slabs  []*RecordList
	slab_m *sync.Mutex
}

func NewLoadSpec() LoadSpec {
	l := LoadSpec{}
	l.files = make(map[string]bool)
	l.columns = make(map[string]bool)

	l.slabs = make([]*RecordList, 0)

	l.slab_m = &sync.Mutex{}
	return l
}

func (t *Table) NewLoadSpec() LoadSpec {
	l := NewLoadSpec()
	l.table = t

	return l
}

func (l *LoadSpec) assert_col_type(name string, col_type int8) {
	if l.table == nil {
		return
	}
	name_id := l.table.get_key_id(name)

	if l.table.KeyTypes[name_id] == 0 {
		Error("Query Error! Column ", name, " does not exist")
	}

	if l.table.KeyTypes[name_id] != col_type {
		var col_type_name string
		switch col_type {
		case INT_VAL:
			col_type_name = "Int"
		case STR_VAL:
			col_type_name = "Str"
		case SET_VAL:
			col_type_name = "Set"
		}

		Error("Query Error! Key ", name, " exists, but is not of type ", col_type_name)
	}
}

func (l *LoadSpec) Str(name string) {
	l.assert_col_type(name, STR_VAL)
	l.columns[name] = true
	l.files["str_"+name+".db"] = true
}
func (l *LoadSpec) Int(name string) {
	l.assert_col_type(name, INT_VAL)
	l.columns[name] = true
	l.files["int_"+name+".db"] = true
}
func (l *LoadSpec) Set(name string) {
	l.assert_col_type(name, SET_VAL)
	l.columns[name] = true
	l.files["set_"+name+".db"] = true
}
