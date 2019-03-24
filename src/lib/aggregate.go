package sybil

import "fmt"
import "time"
import "bytes"
import "sort"
import "strconv"
import "sync"
import "math"
import "runtime"

import "encoding/binary"

var INTERNAL_RESULT_LIMIT = 100000
var GROUP_BY_WIDTH = 8 // bytes

const DISTINCT_STR = "distinct"
const HIST_STR = "hist"
const SORT_COUNT = "$COUNT"

const (
	NO_OP       = ""
	OP_AVG      = "avg"
	OP_HIST     = "hist"
	OP_DISTINCT = "distinct"
)

var GROUP_DELIMITER = "\t"
var MISSING_VALUE = uint64(math.MaxUint64)

type SortResultsByCol struct {
	Results []*Result

	Col string
}

func (a SortResultsByCol) Len() int      { return len(a.Results) }
func (a SortResultsByCol) Swap(i, j int) { a.Results[i], a.Results[j] = a.Results[j], a.Results[i] }

// This sorts the records in descending order
func (a SortResultsByCol) Less(i, j int) bool {
	if a.Col == SORT_COUNT {
		t1 := a.Results[i].Count
		t2 := a.Results[j].Count

		return t1 > t2
	}

	t1 := a.Results[i].Hists[a.Col].Mean()
	t2 := a.Results[j].Hists[a.Col].Mean()
	return t1 > t2
}

func FilterAndAggRecords(querySpec *QuerySpec, recordsPtr *RecordList) int {

	// {{{ variable decls and func setup
	var ok bool
	var binarybuffer []byte = make([]byte, GROUP_BY_WIDTH*len(querySpec.Groups))
	var distinctbuffer []byte = make([]byte, GROUP_BY_WIDTH*len(querySpec.Distincts))
	var slowdistinctbuffer bytes.Buffer

	bs := make([]byte, GROUP_BY_WIDTH)
	zero := make([]byte, GROUP_BY_WIDTH)
	records := *recordsPtr

	var weight = int64(1)

	matched_records := 0
	if HOLD_MATCHES {
		querySpec.Matched = make(RecordList, 0)
	}
	length := len(querySpec.Table.KeyTable)
	columns := make([]*TableColumn, length)
	result_map := querySpec.Results

	// {{{ check if we need to do a count distinct
	do_count_distinct := false
	only_ints_in_distinct := true

	// check whether we can go down the fast path (integer only)
	// or we have to use a slow path when doing count distinct
	if len(querySpec.Distincts) > 0 {
		do_count_distinct = true
		for _, g := range querySpec.Distincts {
			if INT_VAL != querySpec.Table.KeyTypes[g.name_id] {
				only_ints_in_distinct = false
			}
		}
	} // }}} count distinct check

	// }}} func setup

	// {{{ the main loop over all records
	params := make(map[string]interface{})
	for i := 0; i < len(records); i++ {
		add := true
		r := records[i]

		if OPTS.WEIGHT_COL && r.Populated[OPTS.WEIGHT_COL_ID] == INT_VAL {
			weight = int64(r.Ints[OPTS.WEIGHT_COL_ID])
		}

		// {{{ FILTERING
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

		// }}} FILTERING

		// {{{ GROUP BY into a byte buffer using integer values
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
				binary.LittleEndian.PutUint64(bs, MISSING_VALUE)
			}

			copy(binarybuffer[i*GROUP_BY_WIDTH:], bs)
		} // }}}

		// {{{ time series aggregation
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
					big_record = querySpec.NewResult()
					big_record.BinaryByKey = string(binarybuffer)
					querySpec.Results[string(binarybuffer)] = big_record
					b_ok = true
				}
			}

			if b_ok {
				big_record.Samples++
				big_record.Count += weight
			}

			// to do a time series aggregation, we treat each time bucket
			// as its own ResultMap and promote the current time bucket to
			// our result map for this record's aggregation
			val = int64(int(val) / querySpec.TimeBucket * querySpec.TimeBucket)
			result_map, ok = querySpec.TimeResults[int(val)]

			if !ok {
				// TODO: this make call is kind of slow...
				result_map = make(ResultMap)
				querySpec.TimeResults[int(val)] = result_map
			}

		} // }}} time series

		// {{{ group by lookup in our result map
		added_record, ok := result_map[string(binarybuffer)]

		// this finds or creates a Result for the groupbykey
		// we created earlier
		if !ok {
			// TODO: take into account whether we are doint time series or not...
			if len(result_map) >= INTERNAL_RESULT_LIMIT {
				continue
			}

			added_record = querySpec.NewResult()
			added_record.BinaryByKey = string(binarybuffer)

			result_map[string(binarybuffer)] = added_record
		} // }}}

		added_record.Samples++
		added_record.Count += weight

		// {{{ count distinct aggregation
		if do_count_distinct {

			if only_ints_in_distinct {
				// if we are doing a count distinct, lets try to go the fast route
				for i, g := range querySpec.Distincts {
					copy(bs, zero)
					switch r.Populated[g.name_id] {
					case INT_VAL:
						binary.LittleEndian.PutUint64(bs, uint64(r.Ints[g.name_id]))
					case _NO_VAL:
						binary.LittleEndian.PutUint64(bs, MISSING_VALUE)
					}

					copy(distinctbuffer[i*GROUP_BY_WIDTH:], bs)
				}

				added_record.Distinct.Add(distinctbuffer)

			} else {
				// slow path for count distinct on strings
				for _, g := range querySpec.Distincts {
					switch r.Populated[g.name_id] {
					case INT_VAL:
						slowdistinctbuffer.WriteString(strconv.FormatInt(int64(r.Ints[g.name_id]), 10))
					case STR_VAL:
						col := r.block.GetColumnInfo(g.name_id)
						slowdistinctbuffer.WriteString(col.get_string_for_val(int32(r.Strs[g.name_id])))

					}
					slowdistinctbuffer.WriteString(GROUP_DELIMITER)
				}

				added_record.Distinct.Add(slowdistinctbuffer.Bytes())
				slowdistinctbuffer.Reset()

			}

		} // }}}

		// {{{ EXPRESSIONS
		params["r"] = r
		for _, e := range querySpec.Expressions {
			ret, err := e.Expr.Evaluate(params)
			if err != nil {
				continue
			}
			r.Populated[e.name_id] = e.ExprType

			switch v := ret.(type) {
			case int:
				r.Ints[e.name_id] = IntField(v)
				// TODO:
				// case string:
				//	r.Strs[e.name_id] = StrField(v)

			}

		}
		// }}}

		// {{{ aggregations
		for _, a := range querySpec.Aggregations {
			switch r.Populated[a.name_id] {
			case INT_VAL:
				val := int64(r.Ints[a.name_id])

				hist, ok := added_record.Hists[a.Name]

				if !ok {
					hist = r.block.table.NewHist(r.block.table.get_int_info(a.name_id))
					added_record.Hists[a.Name] = hist
				}

				hist.AddWeightedValue(val, weight)
			}

		} // }}}

	} // }}} main record loop

	// {{{ translate group by
	// turn the group by byte buffers into their
	// actual string equivalents.
	if len(querySpec.TimeResults) > 0 {
		for k, result_map := range querySpec.TimeResults {
			querySpec.TimeResults[k] = *translate_group_by(result_map, querySpec.Groups, columns)
		}

	}

	if len(querySpec.Results) > 0 {
		querySpec.Results = *translate_group_by(querySpec.Results, querySpec.Groups, columns)
	}
	// }}}

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
			if val != MISSING_VALUE {
				switch col.Type {
				case INT_VAL:
					buffer.WriteString(strconv.FormatInt(int64(val), 10))
				case STR_VAL:
					buffer.WriteString(col.get_string_for_val(int32(val)))
				}
			}

			buffer.WriteString(GROUP_DELIMITER)

		}

		r.GroupByKey = buffer.String()
		newResults[r.GroupByKey] = r
	}

	return &newResults
}

func CopyQuerySpec(querySpec *QuerySpec) *QuerySpec {
	blockQuery := QuerySpec{QueryParams: querySpec.QueryParams}
	blockQuery.Table = querySpec.Table
	blockQuery.Punctuate()

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

func CombineAndPrune(querySpec *QuerySpec, block_specs map[string]*QuerySpec) *QuerySpec {

	for _, spec := range block_specs {
		spec.SortResults(spec.PruneBy)
		spec.PruneResults(FLAGS.LIMIT)
	}

	resultSpec := CombineResults(querySpec, block_specs)
	resultSpec.SortResults(resultSpec.PruneBy)
	resultSpec.PruneResults(FLAGS.LIMIT)

	return resultSpec
}

func MultiCombineResults(querySpec *QuerySpec, block_specs map[string]*QuerySpec) *QuerySpec {
	num_specs := len(block_specs)
	num_procs := runtime.NumCPU()

	per_block := num_specs / num_procs

	if per_block < 4 {
		return CombineResults(querySpec, block_specs)
	}

	all_results := make([]*QuerySpec, 0)
	next_specs := make(map[string]*QuerySpec)
	m := &sync.Mutex{}
	var wg sync.WaitGroup

	count := 0

	for k, spec := range block_specs {

		next_specs[k] = spec
		count += 1

		if count%per_block == 0 {
			var resultSpec *QuerySpec
			this_specs := next_specs
			next_specs = make(map[string]*QuerySpec)
			wg.Add(1)
			go func() {
				resultSpec = CombineAndPrune(querySpec, this_specs)
				m.Lock()
				all_results = append(all_results, resultSpec)
				m.Unlock()
				wg.Done()
			}()
		}
	}

	wg.Wait()

	agg_specs := make(map[string]*QuerySpec)

	if len(next_specs) > 0 {
		agg_specs["result_last"] = CombineAndPrune(querySpec, next_specs)
	}

	for k, v := range all_results {
		agg_specs[fmt.Sprintf("result_%v", k)] = v
	}

	return CombineResults(querySpec, agg_specs)

}

func CombineResults(querySpec *QuerySpec, block_specs map[string]*QuerySpec) *QuerySpec {

	astart := time.Now()
	resultSpec := *CopyQuerySpec(querySpec)

	master_result := make(ResultMap)
	master_time_result := make(map[int]ResultMap)

	cumulative_result := querySpec.NewResult()
	cumulative_result.GroupByKey = "TOTAL"
	if len(querySpec.Groups) > 1 {
		for _, _ = range querySpec.Groups[1:] {
			cumulative_result.GroupByKey += "\t"
		}
	}

	for _, spec := range block_specs {
		master_result.Combine(&spec.Results)
		resultSpec.MatchedCount += spec.MatchedCount

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

	aend := time.Now()
	if DEBUG_TIMING {
		Debug("AGGREGATING", len(block_specs), "BLOCK RESULTS TOOK", aend.Sub(astart))
	}

	return &resultSpec
}

func (qs *QuerySpec) PruneResults(limit int) {
	limit *= 10
	if limit > 1000 {
		limit = 1000
	}

	if len(qs.Sorted) > limit {
		qs.Sorted = qs.Sorted[:limit]
	}

	qs.Results = make(ResultMap)
	for _, res := range qs.Sorted {
		qs.Results[res.GroupByKey] = res
	}

	for time_bucket, results := range qs.TimeResults {
		interim_result := make(ResultMap)
		for _, res := range results {
			_, ok := qs.Results[res.GroupByKey]
			if ok {
				interim_result[res.GroupByKey] = res
			}
		}

		qs.TimeResults[time_bucket] = interim_result
	}
}

func (qs *QuerySpec) SortResults(orderBy string) {
	// SORT THE RESULTS
	if orderBy != "" {
		start := time.Now()
		sorter := SortResultsByCol{}
		sorter.Results = make([]*Result, 0)
		for _, v := range qs.Results {
			sorter.Results = append(sorter.Results, v)
		}
		qs.Sorted = sorter.Results

		sorter.Col = qs.OrderBy
		sort.Sort(sorter)

		end := time.Now()
		if DEBUG_TIMING {
			Debug("SORTING TOOK", end.Sub(start))
		}

		qs.Sorted = sorter.Results
	}

}

// OLD SEARCHING FUNCTIONS BELOW HERE
func SearchBlocks(querySpec *QuerySpec, block_list map[string]*TableBlock) map[string]*QuerySpec {
	var wg sync.WaitGroup
	// Each block gets its own querySpec (for locking and combining purposes)
	// after all queries finish executing, the specs are combined
	block_specs := make(map[string]*QuerySpec, len(block_list))

	// DONE: why iterate through blocklist after loading it instead of filtering
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

	querySpec.SortResults(querySpec.OrderBy)

	Debug(string(len(matched)), "RECORDS FILTERED AND AGGREGATED INTO", len(querySpec.Results), "RESULTS, TOOK", end.Sub(start))

}

// vim: foldmethod=marker
