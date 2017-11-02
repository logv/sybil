package structs

import "sync"

// this metadata is saved into <table>/<block>/info.db
type SavedTableBlockInfo struct {
	Name string
	Info *SavedColumnInfo
	Size int64

	IntInfo IntInfoTable
	StrInfo StrInfoTable
}

type TableBlock struct {
	SavedTableBlockInfo

	RecordList RecordList
	Matched    RecordList

	Table         *Table
	StringIDMutex *sync.Mutex

	ValStringIDLookup map[int32]string
	Columns           map[int16]*TableColumn
}

func NewTableBlock() TableBlock {

	tb := TableBlock{}
	tb.Columns = make(map[int16]*TableColumn)
	tb.ValStringIDLookup = make(map[int32]string)
	tb.StringIDMutex = &sync.Mutex{}

	return tb

}
