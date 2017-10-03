package table_block

import . "github.com/logv/sybil/src/lib/structs"
import . "github.com/logv/sybil/src/lib/metadata"

func GetBlockKeyID(tb *TableBlock, name string) int16 {
	return GetTableKeyID(tb.Table, name)
}
