package pcs

import "sync"

// Table Block should have a bunch of metadata next to it, too
type TableBlock struct {
	Name       string
	RecordList RecordList
	Info       *SavedColumnInfo
	Size       int64
	Matched    RecordList

	string_id_m          *sync.Mutex
	val_string_id_lookup map[int32]string
	table                *Table

	columns map[int16]*TableColumn
}

func newTableBlock() TableBlock {

	tb := TableBlock{}
	tb.columns = make(map[int16]*TableColumn)
	tb.val_string_id_lookup = make(map[int32]string)
	tb.string_id_m = &sync.Mutex{}

	return tb

}

func (tb *TableBlock) get_key_id(name string) int16 {
	return tb.table.get_key_id(name)
}

func (tb *TableBlock) get_string_for_key(id int16) string {
	return tb.table.get_string_for_key(int(id))

}
