package sybil

import "time"

func (tb *TableBlock) allocateRecords(loadSpec *LoadSpec, info SavedColumnInfo, loadRecords bool) RecordList {

	if *FLAGS.RecycleMem && info.NumRecords == int32(ChunkSize) && loadSpec != nil && !loadRecords {
		loadSpec.slabMutex.Lock()
		defer loadSpec.slabMutex.Unlock()
		if len(loadSpec.slabs) > 0 {
			slab := loadSpec.slabs[0]
			loadSpec.slabs = loadSpec.slabs[1:]

			slab.ResetRecords(tb)
			tb.RecordList = *slab
			return *slab
		}
	}

	slab := tb.makeRecordSlab(loadSpec, info, loadRecords)
	return slab

}

func (tb *TableBlock) makeRecordSlab(loadSpec *LoadSpec, info SavedColumnInfo, loadRecords bool) RecordList {
	t := tb.table

	var r *Record

	var records RecordList
	var alloced []Record
	var bigIntArr IntArr
	var bigStrArr StrArr
	var bigPopArr []int8
	var hasSets = false
	var hasStrs = false
	var hasInts = false
	maxKeyID := 0
	for _, v := range t.KeyTable {
		if maxKeyID <= int(v) {
			maxKeyID = int(v) + 1
		}
	}

	// determine if we need to allocate the different field containers inside
	// each record
	if loadSpec != nil && loadRecords == false {
		for fieldName := range loadSpec.columns {
			v := t.getKeyID(fieldName)

			switch t.KeyTypes[v] {
			case IntVal:
				hasInts = true
			case SetVal:
				hasSets = true
			case StrVal:
				hasStrs = true
			default:
				Error("MISSING KEY TYPE FOR COL", v)
			}
		}
	} else {
		hasSets = true
		hasInts = true
		hasStrs = true
	}

	if loadSpec != nil || loadRecords {
		mstart := time.Now()
		records = make(RecordList, info.NumRecords)
		alloced = make([]Record, info.NumRecords)
		if hasInts {
			bigIntArr = make(IntArr, maxKeyID*int(info.NumRecords))
		}
		if hasStrs {
			bigStrArr = make(StrArr, maxKeyID*int(info.NumRecords))
		}
		bigPopArr = make([]int8, maxKeyID*int(info.NumRecords))
		mend := time.Now()

		if DebugTiming {
			Debug("MALLOCED RECORDS", info.NumRecords, "TOOK", mend.Sub(mstart))
		}

		start := time.Now()
		for i := range records {
			r = &alloced[i]
			if hasInts {
				r.Ints = bigIntArr[i*maxKeyID : (i+1)*maxKeyID]
			}

			if hasStrs {
				r.Strs = bigStrArr[i*maxKeyID : (i+1)*maxKeyID]
			}

			// TODO: move this allocation next to the allocations above
			if hasSets {
				r.SetMap = make(SetMap)
			}

			r.Populated = bigPopArr[i*maxKeyID : (i+1)*maxKeyID]

			r.block = tb
			records[i] = r
		}
		end := time.Now()

		if DebugTiming {
			Debug("INITIALIZED RECORDS", info.NumRecords, "TOOK", end.Sub(start))
		}
	}

	tb.RecordList = records[:]
	return tb.RecordList

}

// recycle allocated records between blocks
// that means we need a wash and rinse cycle
// we can re-use blocks if:
//   same loadSpec
//   table is the same
//   NumRecords are the same
// to do so,
//   we clean out the different arrays inside a block
//   re-home the record list into the table block
func (rl RecordList) ResetRecords(tb *TableBlock) {
	if len(rl) <= 0 {
		return
	}

	for _, record := range rl {
		if record.Ints != nil {
			for i := range record.Ints {
				record.Ints[i] = 0
			}
		}

		if record.Strs != nil {
			for i := range record.Strs {
				record.Strs[i] = 0
			}
		}

		if record.SetMap != nil {
			record.SetMap = make(SetMap)
		}

		for i := range record.Populated {
			record.Populated[i] = _NoVal
		}

		record.block = tb

	}

}

func (tb *TableBlock) RecycleSlab(loadSpec *LoadSpec) {
	if *FLAGS.RecycleMem {
		rl := tb.RecordList

		if len(rl) == ChunkSize {
			loadSpec.slabMutex.Lock()
			loadSpec.slabs = append(loadSpec.slabs, &rl)
			loadSpec.slabMutex.Unlock()
		}
	}
}
