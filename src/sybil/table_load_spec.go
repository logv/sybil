package sybil

import (
	"sync"

	"github.com/pkg/errors"
)

type LoadSpec struct {
	columns map[string]bool
	files   map[string]bool

	LoadAllColumns             bool
	ReadRowsOnly               bool
	SkipDeleteBlocksAfterQuery bool

	table *Table

	slabs  []*RecordList
	slabMu *sync.Mutex
}

func NewLoadSpec() LoadSpec {
	l := LoadSpec{}
	l.files = make(map[string]bool)
	l.columns = make(map[string]bool)

	l.slabs = make([]*RecordList, 0)

	l.slabMu = &sync.Mutex{}
	return l
}

func (t *Table) NewLoadSpec() LoadSpec {
	l := NewLoadSpec()
	l.table = t

	return l
}

func (l *LoadSpec) checkColType(name string, colType int8) error {
	if l.table == nil {
		return nil
	}
	nameID := l.table.getKeyID(name)

	if l.table.KeyTypes[nameID] == 0 {
		return ErrMissingColumn{name}
	}

	if l.table.KeyTypes[nameID] != colType {
		var colTypeName string
		switch colType {
		case INT_VAL:
			colTypeName = "Int"
		case STR_VAL:
			colTypeName = "Str"
		case SET_VAL:
			colTypeName = "Set"
		}
		return ErrColumnTypeMismatch{name, colTypeName}
	}
	return nil
}

func (l *LoadSpec) Str(name string) error {
	if err := l.checkColType(name, STR_VAL); err != nil {
		return errors.Wrap(err, "Str")
	}
	l.columns[name] = true
	l.files["str_"+name+".db"] = true
	return nil
}
func (l *LoadSpec) Int(name string) error {
	if err := l.checkColType(name, INT_VAL); err != nil {
		return errors.Wrap(err, "Int")
	}
	l.columns[name] = true
	l.files["int_"+name+".db"] = true
	return nil
}
func (l *LoadSpec) Set(name string) error {
	if err := l.checkColType(name, SET_VAL); err != nil {
		return errors.Wrap(err, "Set")
	}
	l.columns[name] = true
	l.files["set_"+name+".db"] = true
	return nil
}
