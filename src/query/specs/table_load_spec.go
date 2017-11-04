package sybil

import (
	"sync"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/structs"

	md "github.com/logv/sybil/src/lib/metadata"
)

type LoadSpec struct {
	Columns map[string]bool
	Files   map[string]bool

	LoadAllColumns bool
	Table          *Table

	Slabs     []*RecordList
	SlabMutex *sync.Mutex
}

func NewLoadSpec() LoadSpec {
	l := LoadSpec{}
	l.Files = make(map[string]bool)
	l.Columns = make(map[string]bool)

	l.Slabs = make([]*RecordList, 0)

	l.SlabMutex = &sync.Mutex{}
	return l
}

func NewTableLoadSpec(t *Table) LoadSpec {
	l := NewLoadSpec()
	l.Table = t

	return l
}

func (l *LoadSpec) assert_col_type(name string, col_type int8) {
	if l.Table == nil {
		return
	}
	NameID := md.GetTableKeyID(l.Table, name)

	if l.Table.KeyTypes[NameID] == 0 {
		Error("Query Error! Column ", name, " does not exist")
	}

	if l.Table.KeyTypes[NameID] != col_type {
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
	l.Columns[name] = true
	// gob 1
	l.Files["str_"+name+".db"] = true

	// gob 2
	l.Files[name+".str.gob"] = true
	l.Files[name+".str.db"] = true
	// protofub
	l.Files[name+".str.pb"] = true
}
func (l *LoadSpec) Int(name string) {
	l.assert_col_type(name, INT_VAL)
	l.Columns[name] = true

	// gob ver 1
	l.Files["int_"+name+".db"] = true

	// gob ver 2
	l.Files[name+".int.gob"] = true
	l.Files[name+".int.db"] = true

	// protofub
	l.Files[name+".int.pb"] = true

}
func (l *LoadSpec) Set(name string) {
	l.assert_col_type(name, SET_VAL)
	l.Columns[name] = true
	l.Files["set_"+name+".db"] = true

	l.Files[name+".set.gob"] = true
	l.Files[name+".set.db"] = true
	// protobuf
	l.Files[name+".set.pb"] = true
}
