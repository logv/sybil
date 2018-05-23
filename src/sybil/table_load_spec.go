package sybil

import "sync"

type LoadSpec struct {
	columns map[string]bool
	files   map[string]bool

	LoadAllColumns bool
	table          *Table

	slabs []*RecordList
	slabM *sync.Mutex
}

func NewLoadSpec() LoadSpec {
	l := LoadSpec{}
	l.files = make(map[string]bool)
	l.columns = make(map[string]bool)

	l.slabs = make([]*RecordList, 0)

	l.slabM = &sync.Mutex{}
	return l
}

func (t *Table) NewLoadSpec() LoadSpec {
	l := NewLoadSpec()
	l.table = t

	return l
}

func (l *LoadSpec) assertColType(name string, colType int8) {
	if l.table == nil {
		return
	}
	nameId := l.table.getKeyId(name)

	if l.table.KeyTypes[nameId] == 0 {
		Error("Query Error! Column ", name, " does not exist")
	}

	if l.table.KeyTypes[nameId] != colType {
		var colTypeName string
		switch colType {
		case INT_VAL:
			colTypeName = "Int"
		case STR_VAL:
			colTypeName = "Str"
		case SET_VAL:
			colTypeName = "Set"
		}

		Error("Query Error! Key ", name, " exists, but is not of type ", colTypeName)
	}
}

func (l *LoadSpec) Str(name string) {
	l.assertColType(name, STR_VAL)
	l.columns[name] = true
	l.files["str_"+name+".db"] = true
}
func (l *LoadSpec) Int(name string) {
	l.assertColType(name, INT_VAL)
	l.columns[name] = true
	l.files["int_"+name+".db"] = true
}
func (l *LoadSpec) Set(name string) {
	l.assertColType(name, SET_VAL)
	l.columns[name] = true
	l.files["set_"+name+".db"] = true
}
