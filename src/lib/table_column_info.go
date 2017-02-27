package sybil

import "sort"
import "math"


// THIS FILE HAS BOOKKEEPING FOR COLUMN DATA ON A TABLE AND BLOCK BASIS
// it adds update_int_info and update_str_info to Table/TableBlock

// TODO: collapse the IntInfo and StrInfo into fields on tableColumn

// StrInfo and IntInfo contains interesting tidbits about columns
// they also get serialized to disk in the block's info.db
type StrInfo struct {
	TopStringCount map[int32]int
	Cardinality    int
}

type IntInfo struct {
	Min   int64
	Max   int64
	Avg   float64
	M2    float64 // used for calculating std dev, expressed as M2 / (Count - 1)
	Count int
}

type IntInfoTable map[int16]*IntInfo
type StrInfoTable map[int16]*StrInfo

var TOP_STRING_COUNT = 20

type StrInfoCol struct {
	Name  int32
	Value int
}

type SortStrsByCount []StrInfoCol

func (a SortStrsByCount) Len() int           { return len(a) }
func (a SortStrsByCount) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortStrsByCount) Less(i, j int) bool { return a[i].Value < a[j].Value }

func (si *StrInfo) prune() {
	si.Cardinality = len(si.TopStringCount)

	if si.Cardinality > TOP_STRING_COUNT {
		interim := make([]StrInfoCol, 0)

		for s, c := range si.TopStringCount {
			interim = append(interim, StrInfoCol{Name: s, Value: c})
		}

		sort.Sort(SortStrsByCount(interim))

		for _, x := range interim[:len(si.TopStringCount)-TOP_STRING_COUNT-1] {
			delete(si.TopStringCount, x.Name)
		}
	}

}

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

func (t *Table) update_int_info(name int16, val int64) {
	update_int_info(t.IntInfo, name, val)
}

func (tb *TableBlock) update_str_info(name int16, val int, increment int) {
	if tb.StrInfo == nil {
		tb.StrInfo = make(map[int16]*StrInfo)
	}

	update_str_info(tb.StrInfo, name, val, increment)
}

func (tb *TableBlock) update_int_info(name int16, val int64) {
	if tb.IntInfo == nil {
		tb.IntInfo = make(map[int16]*IntInfo)
	}

	update_int_info(tb.IntInfo, name, val)
}

func (t *Table) get_int_info(name int16) *IntInfo {
	return t.IntInfo[name]

}

func (tb *TableBlock) get_int_info(name int16) *IntInfo {
	return tb.IntInfo[name]
}

func (tb *TableBlock) get_str_info(name int16) *StrInfo {
	return tb.StrInfo[name]
}
