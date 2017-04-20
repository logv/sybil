package sybil

import "time"
import "bytes"
import "sort"
import "strconv"
import "sync"
import "math"

import "encoding/binary"

var INTERNAL_RESULT_LIMIT = 100000
var GROUP_BY_WIDTH = 8 // bytes

const (
	NO_OP       = iota
	OP_AVG      = iota
	OP_HIST     = iota
	OP_DISTINCT = iota
)

var GROUP_DELIMITER = "\t"

type SortResultsByCol struct {
	Results []*Result

	Col string
}

func (a SortResultsByCol) Len() int      { return len(a.Results) }
func (a SortResultsByCol) Swap(i, j int) { a.Results[i], a.Results[j] = a.Results[j], a.Results[i] }

// This sorts the records in descending order
func (a SortResultsByCol) Less(i, j int) bool {
	if a.Col == OPTS.SORT_COUNT {
		t1 := a.Results[i].Count
		t2 := a.Results[j].Count

		return t1 > t2
	}

	if *FLAGS.OP == "hist" {
		t1 := a.Results[i].Hists[a.Col].Avg
		t2 := a.Results[j].Hists[a.Col].Avg
		return t1 > t2

	}

	t1 := a.Results[i].Hists[a.Col].Avg
	t2 := a.Results[j].Hists[a.Col].Avg
	return t1 > t2
}

func FilterAndAggRecords(querySpec *QuerySpec, recordsPtr *RecordList) int {
	var ok bool
	var binarybuffer []byte = make([]byte, GROUP_BY_WIDTH*len(querySpec.Groups))

	bs := make([]byte, GROUP_BY_WIDTH)
	zero := make([]byte, GROUP_BY_WIDTH)
	records := *recordsPtr

	var weight = int64(1)

	matched_records := 0
	if HOLD_MATCHES {
		querySpec.Matched = make(RecordList, 0)
	}

	var result_map ResultMap
	length := len(querySpec.Table.KeyTable)
	columns := make([]*TableColumn, length)

	if querySpec.TimeBucket <= 0 {
		result_map = querySpec.Results
	}

	for i := 0; i < len(records); i++ {
		add := true
		r := records[i]

		if OPTS.WEIGHT_COL && r.Populated[OPTS.WEIGHT_COL_ID] == INT_VAL {
			weight = int64(r.Ints[OPTS.WEIGHT_COL_ID])
		}

		// FILTERING
		for j := 0; j < len(querySpec.Filters); j++ {
			// returns True if the record matches!
			ret := querySpec.Filters[j].Filter(r) != true
			if ret {
				add = false
				break
			}
		}

		if !add {
			continue
		}

		matched_records++
		if HOLD_MATCHES {
			querySpec.Matched = append(querySpec.Matched, r)
		}

		if *FLAGS.LUA {
			continue
		}

		for i, g := range querySpec.Groups {
			copy(bs, zero)

			if columns[g.name_id] == nil && r.Populated[g.name_id] != _NO_VAL {
				columns[g.name_id] = r.block.GetColumnInfo(g.name_id)
				columns[g.name_id].Type = r.Populated[g.name_id]
			}

			switch r.Populated[g.name_id] {
			case INT_VAL:
				binary.LittleEndian.PutUint64(bs, uint64(r.Ints[g.name_id]))
			case STR_VAL:
				binary.LittleEndian.PutUint64(bs, uint64(r.Strs[g.name_id]))
			case _NO_VAL:
				binary.LittleEndian.PutUint64(bs, math.MaxUint64)
			}

			copy(binarybuffer[i*GROUP_BY_WIDTH:], bs)
		}

		// IF WE ARE DOING A TIME SERIES AGGREGATION (WHICH CAN BE SLOWER)
		if querySpec.TimeBucket > 0 {
			if len(r.Populated) <= int(OPTS.TIME_COL_ID) {
				continue
			}

			if r.Populated[OPTS.TIME_COL_ID] != INT_VAL {
				continue
			}
			val := int64(r.Ints[OPTS.TIME_COL_ID])

			big_record, b_ok := querySpec.Results[string(binarybuffer)]
			if !b_ok {
				if len(querySpec.Results) < INTERNAL_RESULT_LIMIT {
					big_record = NewResult()
					big_record.BinaryByKey = string(binarybuffer)
					querySpec.Results[string(binarybuffer)] = big_record
					b_ok = true
				}
			}

			if b_ok {
				big_record.Samples++
				big_record.Count += weight
			}

			val = int64(int(val) / querySpec.TimeBucket * querySpec.TimeBucket)
			result_map, ok = querySpec.TimeResults[int(val)]

			if !ok {
				// TODO: this make call is kind of slow...
				result_map = make(ResultMap)
				querySpec.TimeResults[int(val)] = result_map
			}

		}

		added_record, ok := result_map[string(binarybuffer)]

		// BUILD GROUPING RECORD
		if !ok {
			// TODO: take into account whether we are doint time series or not...
			if len(result_map) >= INTERNAL_RESULT_LIMIT {
				continue
			}

			added_record = NewResult()
			added_record.BinaryByKey = string(binarybuffer)

			result_map[string(binarybuffer)] = added_record
		}

		added_record.Samples++
		added_record.Count += weight

		// GO THROUGH AGGREGATIONS AND REALIZE THEM
		for _, a := range querySpec.Aggregations {
			switch r.Populated[a.name_id] {
			case INT_VAL:
				val := int64(r.Ints[a.name_id])

				hist, ok := added_record.Hists[a.name]

				if !ok {
					hist = r.block.table.NewHist(r.block.table.get_int_info(a.name_id))
					if a.op_id == OP_HIST {
						hist.TrackPercentiles()
					}

					added_record.Hists[a.name] = hist
				}

				hist.addWeightedValue(val, weight)
			}

		}

	}

	// Now to unpack the byte buffers we oh so stupidly used in the group by...

	if len(querySpec.TimeResults) > 0 {
		for k, result_map := range querySpec.TimeResults {
			querySpec.TimeResults[k] = *translate_group_by(result_map, querySpec.Groups, columns)
		}

	}

	if len(querySpec.Results) > 0 {
		querySpec.Results = *translate_group_by(querySpec.Results, querySpec.Groups, columns)
	}

	if *FLAGS.LUA {
		querySpec.luaInit()
		querySpec.luaMap(&querySpec.Matched)
	}

	return matched_records

}

func translate_group_by(Results ResultMap, Groups []Grouping, columns []*TableColumn) *ResultMap {

	var buffer bytes.Buffer

	var newResults = make(ResultMap)
	var bs []byte

	for _, r := range Results {
		buffer.Reset()
		if len(Groups) == 0 {
			buffer.WriteString("total")
		}
		for i, g := range Groups {
			bs = []byte(r.BinaryByKey[i*GROUP_BY_WIDTH : (i+1)*GROUP_BY_WIDTH])

			col := columns[g.name_id]

			if col == nil {
				buffer.WriteString(GROUP_DELIMITER)
				continue
			}

			val := binary.LittleEndian.Uint64(bs)
			switch col.Type {
			case INT_VAL:
				buffer.WriteString(strconv.FormatInt(int64(val), 10))
			case STR_VAL:
				buffer.WriteString(col.get_string_for_val(int32(val)))

			}

			buffer.WriteString(GROUP_DELIMITER)

		}

		r.GroupByKey = buffer.String()
		newResults[r.GroupByKey] = r
	}

	return &newResults
}

func CopyQuerySpec(querySpec *QuerySpec) *QuerySpec {
	blockQuery := QuerySpec{}
	blockQuery.Table = querySpec.Table
	blockQuery.Punctuate()
	blockQuery.TimeBucket = querySpec.TimeBucket
	blockQuery.Filters = querySpec.Filters
	blockQuery.Aggregations = querySpec.Aggregations
	blockQuery.Groups = querySpec.Groups

	return &blockQuery
}

func CombineMatches(block_specs map[string]*QuerySpec) RecordList {
	start := time.Now()
	matched := make(RecordList, 0)
	for _, spec := range block_specs {
		matched = append(matched, spec.Matched...)
	}
	end := time.Now()

	Debug("JOINING", len(matched), "MATCHED RECORDS TOOK", end.Sub(start))
	return matched

}

func CombineResults(querySpec *QuerySpec, block_specs map[string]*QuerySpec) *QuerySpec {

	astart := time.Now()
	resultSpec := QuerySpec{}
	resultSpec.Table = querySpec.Table
	resultSpec.LuaResult = make(LuaTable, 0)

	if *FLAGS.LUA {
		resultSpec.luaInit()
	}

	master_result := make(ResultMap)
	master_time_result := make(map[int]ResultMap)

	cumulative_result := NewResult()
	cumulative_result.GroupByKey = "TOTAL"
	if len(querySpec.Groups) > 1 {
		for _, _ = range querySpec.Groups[1:] {
			cumulative_result.GroupByKey += "\t"
		}
	}

	for _, spec := range block_specs {
		master_result.Combine(&spec.Results)

		if *FLAGS.LUA {
			resultSpec.luaCombine(spec)
		}

		for _, result := range spec.Results {
			cumulative_result.Combine(result)
		}

		for i, v := range spec.TimeResults {
			mval, ok := master_time_result[i]

			if !ok {
				master_time_result[i] = v
			} else {
				for k, r := range v {
					mh, ok := mval[k]
					if ok {
						mh.Combine(r)
					} else {
						mval[k] = r
					}
				}
			}
		}
	}

	resultSpec.Cumulative = cumulative_result
	resultSpec.TimeBucket = querySpec.TimeBucket
	resultSpec.TimeResults = master_time_result
	resultSpec.Results = master_result

	if *FLAGS.LUA {
		resultSpec.luaFinalize()
	}

	aend := time.Now()
	Debug("AGGREGATING", len(block_specs), "BLOCK RESULTS TOOK", aend.Sub(astart))

	return &resultSpec
}

func SortResults(querySpec *QuerySpec) {
	// SORT THE RESULTS
	if querySpec.OrderBy != "" {
		start := time.Now()
		sorter := SortResultsByCol{}
		sorter.Results = make([]*Result, 0)
		for _, v := range querySpec.Results {
			sorter.Results = append(sorter.Results, v)
		}
		querySpec.Sorted = sorter.Results

		sorter.Col = querySpec.OrderBy
		sort.Sort(sorter)

		end := time.Now()
		if DEBUG_TIMING {
			Debug("SORTING TOOK", end.Sub(start))
		}

		if len(sorter.Results) > *FLAGS.LIMIT {
			sorter.Results = sorter.Results[:*FLAGS.LIMIT]
		}

		querySpec.Sorted = sorter.Results
	}

}

// OLD SEARCHING FUNCTIONS BELOW HERE
func SearchBlocks(querySpec *QuerySpec, block_list map[string]*TableBlock) map[string]*QuerySpec {
	var wg sync.WaitGroup
	// Each block gets its own querySpec (for locking and combining purposes)
	// after all queries finish executing, the specs are combined
	block_specs := make(map[string]*QuerySpec, len(block_list))

	// TODO: why iterate through blocklist after loading it instead of filtering
	// and aggregating while loading them? (and then releasing the blocks)
	// That would mean pushing the call to 'FilterAndAggRecords' to the loading area
	spec_lock := sync.Mutex{}
	for _, block := range block_list {
		wg.Add(1)
		this_block := block
		go func() {
			defer wg.Done()

			blockQuery := CopyQuerySpec(querySpec)

			FilterAndAggRecords(blockQuery, &this_block.RecordList)

			spec_lock.Lock()
			block_specs[this_block.Name] = blockQuery
			spec_lock.Unlock()

		}()
	}

	wg.Wait()

	return block_specs
}

func (t *Table) MatchAndAggregate(querySpec *QuerySpec) {
	start := time.Now()

	querySpec.Table = t
	block_specs := SearchBlocks(querySpec, t.BlockList)
	querySpec.ResetResults()

	// COMBINE THE PER BLOCK RESULTS
	resultSpec := CombineResults(querySpec, block_specs)

	aend := time.Now()
	Debug("AGGREGATING TOOK", aend.Sub(start))

	querySpec.Results = resultSpec.Results
	querySpec.TimeResults = resultSpec.TimeResults

	// Aggregating Matched Records
	matched := CombineMatches(block_specs)
	if HOLD_MATCHES {
		querySpec.Matched = matched
	}

	end := time.Now()

	SortResults(querySpec)

	Debug(string(len(matched)), "RECORDS FILTERED AND AGGREGATED INTO", len(querySpec.Results), "RESULTS, TOOK", end.Sub(start))

}
