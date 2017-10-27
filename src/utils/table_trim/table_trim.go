package sybil

import (
	"sort"

	. "github.com/logv/sybil/src/exp/query_sessions"
	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/query/load_and_query"
	. "github.com/logv/sybil/src/storage/column_store"
)

type TrimSpec struct {
	MBLimit      int64 // size limit of DB in megabytes
	DeleteBefore int64 // delete records older than DeleteBefore in seconds
}

// List all the blocks that should be trimmed to keep the table within it's
// memory limits
func TrimTable(t *Table, trimSpec *TrimSpec) []*TableBlock {
	LoadRecords(t, nil)
	Debug("TRIMMING TABLE, MEMORY LIMIT", trimSpec.MBLimit, "TIME LIMIT", trimSpec.DeleteBefore)

	blocks := make([]*TableBlock, 0)
	to_trim := make([]*TableBlock, 0)

	for _, b := range t.BlockList {
		if b.Name == ROW_STORE_BLOCK {
			continue
		}

		block := LoadBlockFromDir(t, b.Name, nil, false)
		if block != nil {
			if block.Info.IntInfoMap[*FLAGS.TIME_COL] != nil {
				block.Table = t
				blocks = append(blocks, block)
			}
		}
	}

	// Sort the blocks by descending Max Time
	sort.Sort(sort.Reverse(SortBlocksByEndTime(blocks)))

	size := int64(0)
	bytes_in_megabytes := int64(1024 * 1024)
	for _, b := range blocks {

		info := b.Info.IntInfoMap[*FLAGS.TIME_COL]
		trim := false
		if trimSpec.MBLimit > 0 && size/bytes_in_megabytes >= trimSpec.MBLimit {
			trim = true
		}

		if info.Max < trimSpec.DeleteBefore {
			trim = true
		}

		if trim {
			to_trim = append(to_trim, b)
		}

		size += b.Size
	}

	return to_trim
}
