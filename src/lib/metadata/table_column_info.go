package metadata

import (
	"math"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/structs"
)

func update_str_info(str_info_table map[int16]*StrInfo, name int16, val, increment int) {
	info, ok := str_info_table[name]
	if !ok {
		info = &StrInfo{}
		info.TopStringCount = make(map[int32]int)
		str_info_table[name] = info
	}

	info.TopStringCount[int32(val)] += increment
}

var STD_CUTOFF = 1000.0 // if value is 1000 SDs away, we ignore it
var MIN_CUTOFF = 5      // need at least this many elements before we determine min/max

func update_int_info(int_info_table map[int16]*IntInfo, name int16, val int64) {
	info, ok := int_info_table[name]
	if !ok {
		info = &IntInfo{}
		int_info_table[name] = info
		info.Max = val
		info.Min = val
		info.Avg = float64(val)
		info.Count = 1
	}

	delta := float64(val) - info.Avg
	stddev := info.M2 / float64(info.Count-1)
	if stddev <= 1 {
		stddev = math.Max(info.Avg, 1.0) // assume large standard deviation early on
	}

	ignored := false

	if info.Max < val {
		// calculate how off the current value is from mean in terms of our
		// standard deviation and decide whether it is an extreme outlier or not
		delta_in_stddev := math.Abs(delta) / stddev

		if (delta_in_stddev < STD_CUTOFF && info.Count > MIN_CUTOFF) || *FLAGS.SKIP_OUTLIERS == false {
			info.Max = val
		} else {
			ignored = true

			if info.Count > MIN_CUTOFF {
				Debug("IGNORING MAX VALUE", val, "AVG IS", info.Avg, "DELTA / STD", delta_in_stddev)
			}
		}
	}

	if info.Min > val {
		delta_in_stddev := math.Abs(delta) / stddev

		if (delta_in_stddev < STD_CUTOFF && info.Count > MIN_CUTOFF) || *FLAGS.SKIP_OUTLIERS == false {
			info.Min = val
		} else {
			ignored = true
			if info.Count > MIN_CUTOFF {
				Debug("IGNORING MIN VALUE", val, "AVG IS", info.Avg, "DELTA / STD", delta_in_stddev)
			}
		}
	}

	if ignored == false || info.Count < MIN_CUTOFF {
		info.Avg = info.Avg + delta/float64(info.Count)

		// for online variance calculation
		info.M2 = info.M2 + delta*(float64(val)-info.Avg)

	}
	info.Count++
}

func UpdateTableIntInfo(t *Table, name int16, val int64) {
	update_int_info(t.IntInfo, name, val)
}

func GetTableIntInfo(t *Table, name int16) *IntInfo {
	return t.IntInfo[name]

}

func GetColumnValID(tc *TableColumn, name string) int32 {

	id, ok := tc.StringTable[name]

	if ok {
		return int32(id)
	}

	tc.StringIDMutex.Lock()
	tc.StringTable[name] = int32(len(tc.StringTable))
	tc.ValStringIDLookup[tc.StringTable[name]] = name
	tc.StringIDMutex.Unlock()
	return tc.StringTable[name]
}

func GetColumnStringForVal(tc *TableColumn, id int32) string {
	val, _ := tc.ValStringIDLookup[id]
	return val
}

func GetColumnStringForKey(tc *TableColumn, id int) string {
	return GetBlockStringForKey(tc.Block, int16(id))
}
