package sybil

import (
	. "github.com/logv/sybil/src/lib/metadata"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/query/specs"
	. "github.com/logv/sybil/src/storage/metadata_io"
)

// optimizing for integer pre-cached info
func ShouldLoadBlockFromDir(t *Table, dirname string, querySpec *QuerySpec) bool {
	if querySpec == nil {
		return true
	}

	info := LoadBlockInfo(t, dirname)

	max_record := Record{Ints: IntArr{}, Strs: StrArr{}}
	min_record := Record{Ints: IntArr{}, Strs: StrArr{}}

	if len(info.IntInfoMap) == 0 {
		return true
	}

	for field_name := range info.StrInfoMap {
		field_id := GetTableKeyID(t, field_name)
		ResizeFields(&min_record, field_id)
		ResizeFields(&max_record, field_id)
	}

	for field_name, field_info := range info.IntInfoMap {
		field_id := GetTableKeyID(t, field_name)
		ResizeFields(&min_record, field_id)
		ResizeFields(&max_record, field_id)

		min_record.Ints[field_id] = IntField(field_info.Min)
		max_record.Ints[field_id] = IntField(field_info.Max)

		min_record.Populated[field_id] = INT_VAL
		max_record.Populated[field_id] = INT_VAL
	}

	add := true
	for _, f := range querySpec.Filters {
		// make the minima record and the maxima records...
		switch fil := f.(type) {
		case IntFilter:
			if fil.Op == "gt" || fil.Op == "lt" {
				if f.Filter(&min_record) != true && f.Filter(&max_record) != true {
					add = false
					break
				}
			}
		}
	}

	return add
}
