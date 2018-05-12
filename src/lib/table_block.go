package sybil

import "sync"

// Table Block should have a bunch of metadata next to it, too
type TableBlock struct {
	Name       string
	RecordList RecordList
	Info       *SavedColumnInfo
	Size       int64
	Matched    RecordList

	IntInfo IntInfoTable
	StrInfo StrInfoTable

	table     *Table
	stringIdM *sync.Mutex

	valStringIdLookup map[int32]string
	columns           map[int16]*TableColumn
	brokenKeys        map[string]int16
}

func newTableBlock() TableBlock {

	tb := TableBlock{}
	tb.columns = make(map[int16]*TableColumn)
	tb.valStringIdLookup = make(map[int32]string)
	tb.stringIdM = &sync.Mutex{}

	return tb

}

func (tb *TableBlock) getKeyId(name string) int16 {
	return tb.table.getKeyId(name)
}

func (tb *TableBlock) getStringForKey(id int16) string {
	return tb.table.getStringForKey(int(id))

}
