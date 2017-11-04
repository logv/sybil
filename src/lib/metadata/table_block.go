package metadata

import (
	. "github.com/logv/sybil/src/lib/structs"
)

func GetBlockKeyID(tb *TableBlock, name string) int16 {
	return GetTableKeyID(tb.Table, name)
}
