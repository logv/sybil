package sybil


import "time"

func (tb *TableBlock) allocateRecords(loadSpec *LoadSpec, info SavedColumnInfo, load_records bool) RecordList {

	if *FLAGS.RECYCLE_MEM && info.NumRecords == int32(CHUNK_SIZE) && loadSpec != nil && load_records == false {
		loadSpec.slab_m.Lock()
		defer loadSpec.slab_m.Unlock()
		if len(loadSpec.slabs) > 0 {
			slab := loadSpec.slabs[0]
			loadSpec.slabs = loadSpec.slabs[1:]

			slab.ResetRecords(tb)
			tb.RecordList = *slab
			return *slab
		}
	}

	slab := tb.makeRecordSlab(loadSpec, info, load_records)
	return slab

}

func (tb *TableBlock) makeRecordSlab(loadSpec *LoadSpec, info SavedColumnInfo, load_records bool) RecordList {
	t := tb.table

	var r *Record

	var records RecordList
	var alloced []Record
	var bigIntArr IntArr
	var bigStrArr StrArr
	var bigPopArr []int8
	var has_sets = false
	var has_strs = false
	var has_ints = false
	max_key_id := 0
	for _, v := range t.KeyTable {
		if max_key_id <= int(v) {
			max_key_id = int(v) + 1
		}
	}

	// determine if we need to allocate the different field containers inside
	// each record
	if loadSpec != nil && load_records == false {
		for field_name, _ := range loadSpec.columns {
			v := t.get_key_id(field_name)

			switch t.KeyTypes[v] {
			case INT_VAL:
				has_ints = true
			case SET_VAL:
				has_sets = true
			case STR_VAL:
				has_strs = true
			default:
				Error("MISSING KEY TYPE FOR COL", v)
			}
		}
	} else {
		has_sets = true
		has_ints = true
		has_strs = true
	}

	if loadSpec != nil || load_records {
		mstart := time.Now()
		records = make(RecordList, info.NumRecords)
		alloced = make([]Record, info.NumRecords)
		if has_ints {
			bigIntArr = make(IntArr, max_key_id*int(info.NumRecords))
		}
		if has_strs {
			bigStrArr = make(StrArr, max_key_id*int(info.NumRecords))
		}
		bigPopArr = make([]int8, max_key_id*int(info.NumRecords))
		mend := time.Now()

		if DEBUG_TIMING {
			Debug("MALLOCED RECORDS", info.NumRecords, "TOOK", mend.Sub(mstart))
		}

		start := time.Now()
		for i := range records {
			r = &alloced[i]
			if has_ints {
				r.Ints = bigIntArr[i*max_key_id : (i+1)*max_key_id]
			}

			if has_strs {
				r.Strs = bigStrArr[i*max_key_id : (i+1)*max_key_id]
			}

			// TODO: move this allocation next to the allocations above
			if has_sets {
				r.SetMap = make(SetMap)
			}

			r.Populated = bigPopArr[i*max_key_id : (i+1)*max_key_id]

			r.block = tb
			records[i] = r
		}
		end := time.Now()

		if DEBUG_TIMING {
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
			record.Populated[i] = _NO_VAL
		}

		record.block = tb

	}

}

func (tb *TableBlock) RecycleSlab(loadSpec *LoadSpec) {
	if *FLAGS.RECYCLE_MEM {
		rl := tb.RecordList

		if len(rl) == CHUNK_SIZE {
			loadSpec.slab_m.Lock()
			loadSpec.slabs = append(loadSpec.slabs, &rl)
			loadSpec.slab_m.Unlock()
		}
	}
}
