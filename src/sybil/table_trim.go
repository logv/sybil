package sybil

import "sort"

type TrimSpec struct {
	MBLimit      int64 // size limit of DB in megabytes
	DeleteBefore int64 // delete records older than DeleteBefore in seconds
}

// List all the blocks that should be trimmed to keep the table within it's
// memory limits
func (t *Table) TrimTable(flags *FlagDefs, trimSpec *TrimSpec) []*TableBlock {
	t.LoadRecords(flags, nil)
	Debug("TRIMMING TABLE, MEMORY LIMIT", trimSpec.MBLimit, "TIME LIMIT", trimSpec.DeleteBefore)

	blocks := make([]*TableBlock, 0)
	toTrim := make([]*TableBlock, 0)

	for _, b := range t.BlockList {
		if b.Name == ROW_STORE_BLOCK {
			continue
		}

		block := t.LoadBlockFromDir(flags, b.Name, nil, false)
		if block != nil {
			if block.Info.IntInfoMap[*flags.TIME_COL] != nil {
				block.table = t
				blocks = append(blocks, block)
			}
		}
	}

	// Sort the blocks by descending Max Time
	sort.Sort(sort.Reverse(SortBlocksByEndTime{blocks: blocks, timeCol: trimSpec.TimeColumn}))

	size := int64(0)
	bytesInMegabytes := int64(1024 * 1024)
	for _, b := range blocks {

		info := b.Info.IntInfoMap[*flags.TIME_COL]
		trim := false
		if trimSpec.MBLimit > 0 && size/bytesInMegabytes >= trimSpec.MBLimit {
			trim = true
		}

		if info.Max < trimSpec.DeleteBefore {
			trim = true
		}

		if trim {
			toTrim = append(toTrim, b)
		}

		size += b.Size
	}

	return toTrim
}
