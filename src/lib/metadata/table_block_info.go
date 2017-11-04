package metadata

import (
	. "github.com/logv/sybil/src/lib/structs"
)

func UpdateBlockStrInfo(tb *TableBlock, name int16, val int, increment int) {
	if tb.StrInfo == nil {
		tb.StrInfo = make(map[int16]*StrInfo)
	}

	update_str_info(tb.StrInfo, name, val, increment)
}

func UpdateBlockIntInfo(tb *TableBlock, name int16, val int64) {
	if tb.IntInfo == nil {
		tb.IntInfo = make(map[int16]*IntInfo)
	}

	update_int_info(tb.IntInfo, name, val)
	UpdateTableIntInfo(tb.Table, name, val)
}

func GetBlockIntInfo(tb *TableBlock, name int16) *IntInfo {
	return tb.IntInfo[name]
}

func GetBlockStrInfo(tb *TableBlock, name int16) *StrInfo {
	return tb.StrInfo[name]
}

func GetBlockStringForKey(tb *TableBlock, id int16) string {
	return GetTableStringForKey(tb.Table, int(id))

}

func GetColumnInfo(tb *TableBlock, NameID int16) *TableColumn {
	col, ok := tb.Columns[NameID]
	if !ok {
		col = NewTableColumn(tb)
		tb.Columns[NameID] = col
	}

	return col
}
