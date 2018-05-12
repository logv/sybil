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

var DISTINCT_STR = "distinct"
var HIST_STR = "hist"

const (
	NO_OP       = iota
	OP_AVG      = iota
	OP_HIST     = iota
	OP_DISTINCT = iota
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
	if a.Col == OPTS.SORT_COUNT {
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

	matchedRecords := 0
	if HOLD_MATCHES {
		querySpec.Matched = make(RecordList, 0)
	}
	length := len(querySpec.Table.KeyTable)
	columns := make([]*TableColumn, length)
	resultMap := querySpec.Results

	// {{{ check if we need to do a count distinct
	doCountDistinct := false
	onlyIntsInDistinct := true

	// check whether we can go down the fast path (integer only)
	// or we have to use a slow path when doing count distinct
	if len(querySpec.Distincts) > 0 {
		doCountDistinct = true
		for _, g := range querySpec.Distincts {
			if INT_VAL != querySpec.Table.KeyTypes[g.nameId] {
				onlyIntsInDistinct = false
			}
		}
	} // }}} count distinct check

	// }}} func setup

	// {{{ the main loop over all records
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
		matchedRecords++
		if HOLD_MATCHES {
			querySpec.Matched = append(querySpec.Matched, r)
		}

		// }}} FILTERING

		// {{{ GROUP BY into a byte buffer using integer values
		for i, g := range querySpec.Groups {
			copy(bs, zero)

			if columns[g.nameId] == nil && r.Populated[g.nameId] != _NO_VAL {
				columns[g.nameId] = r.block.GetColumnInfo(g.nameId)
				columns[g.nameId].Type = r.Populated[g.nameId]
			}

			switch r.Populated[g.nameId] {
			case INT_VAL:
				binary.LittleEndian.PutUint64(bs, uint64(r.Ints[g.nameId]))
			case STR_VAL:
				binary.LittleEndian.PutUint64(bs, uint64(r.Strs[g.nameId]))
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

			bigRecord, bOk := querySpec.Results[string(binarybuffer)]
			if !bOk {
				if len(querySpec.Results) < INTERNAL_RESULT_LIMIT {
					bigRecord = querySpec.NewResult()
					bigRecord.BinaryByKey = string(binarybuffer)
					querySpec.Results[string(binarybuffer)] = bigRecord
					bOk = true
				}
			}

			if bOk {
				bigRecord.Samples++
				bigRecord.Count += weight
			}

			// to do a time series aggregation, we treat each time bucket
			// as its own ResultMap and promote the current time bucket to
			// our result map for this record's aggregation
			val = int64(int(val) / querySpec.TimeBucket * querySpec.TimeBucket)
			resultMap, ok = querySpec.TimeResults[int(val)]

			if !ok {
				// TODO: this make call is kind of slow...
				resultMap = make(ResultMap)
				querySpec.TimeResults[int(val)] = resultMap
			}

		} // }}} time series

		// {{{ group by lookup in our result map
		addedRecord, ok := resultMap[string(binarybuffer)]

		// this finds or creates a Result for the groupbykey
		// we created earlier
		if !ok {
			// TODO: take into account whether we are doint time series or not...
			if len(resultMap) >= INTERNAL_RESULT_LIMIT {
				continue
			}

			addedRecord = querySpec.NewResult()
			addedRecord.BinaryByKey = string(binarybuffer)

			resultMap[string(binarybuffer)] = addedRecord
		} // }}}

		addedRecord.Samples++
		addedRecord.Count += weight

		// {{{ count distinct aggregation
		if doCountDistinct {

			if onlyIntsInDistinct {
				// if we are doing a count distinct, lets try to go the fast route
				for i, g := range querySpec.Distincts {
					copy(bs, zero)
					switch r.Populated[g.nameId] {
					case INT_VAL:
						binary.LittleEndian.PutUint64(bs, uint64(r.Ints[g.nameId]))
					case _NO_VAL:
						binary.LittleEndian.PutUint64(bs, MISSING_VALUE)
					}

					copy(distinctbuffer[i*GROUP_BY_WIDTH:], bs)
				}

				addedRecord.Distinct.Add(distinctbuffer)

			} else {
				// slow path for count distinct on strings
				for _, g := range querySpec.Distincts {
					switch r.Populated[g.nameId] {
					case INT_VAL:
						slowdistinctbuffer.WriteString(strconv.FormatInt(int64(r.Ints[g.nameId]), 10))
					case STR_VAL:
						col := r.block.GetColumnInfo(g.nameId)
						slowdistinctbuffer.WriteString(col.getStringForVal(int32(r.Strs[g.nameId])))

					}
					slowdistinctbuffer.WriteString(GROUP_DELIMITER)
				}

				addedRecord.Distinct.Add(slowdistinctbuffer.Bytes())
				slowdistinctbuffer.Reset()

			}

		} // }}}

		// {{{ aggregations
		for _, a := range querySpec.Aggregations {
			switch r.Populated[a.nameId] {
			case INT_VAL:
				val := int64(r.Ints[a.nameId])

				hist, ok := addedRecord.Hists[a.Name]

				if !ok {
					hist = r.block.table.NewHist(r.block.table.getIntInfo(a.nameId))
					addedRecord.Hists[a.Name] = hist
				}

				hist.AddWeightedValue(val, weight)
			}

		} // }}}

	} // }}} main record loop

	// {{{ translate group by
	// turn the group by byte buffers into their
	// actual string equivalents.
	if len(querySpec.TimeResults) > 0 {
		for k, resultMap := range querySpec.TimeResults {
			querySpec.TimeResults[k] = *translateGroupBy(resultMap, querySpec.Groups, columns)
		}

	}

	if len(querySpec.Results) > 0 {
		querySpec.Results = *translateGroupBy(querySpec.Results, querySpec.Groups, columns)
	}
	// }}}

	return matchedRecords

}

func translateGroupBy(Results ResultMap, Groups []Grouping, columns []*TableColumn) *ResultMap {

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

			col := columns[g.nameId]
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
					buffer.WriteString(col.getStringForVal(int32(val)))
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

func CombineMatches(blockSpecs map[string]*QuerySpec) RecordList {
	start := time.Now()
	matched := make(RecordList, 0)
	for _, spec := range blockSpecs {
		matched = append(matched, spec.Matched...)
	}
	end := time.Now()

	Debug("JOINING", len(matched), "MATCHED RECORDS TOOK", end.Sub(start))
	return matched

}

func CombineAndPrune(querySpec *QuerySpec, blockSpecs map[string]*QuerySpec) *QuerySpec {

	for _, spec := range blockSpecs {
		spec.SortResults(spec.PruneBy)
		spec.PruneResults(*FLAGS.LIMIT)
	}

	resultSpec := CombineResults(querySpec, blockSpecs)
	resultSpec.SortResults(resultSpec.PruneBy)
	resultSpec.PruneResults(*FLAGS.LIMIT)

	return resultSpec
}

func MultiCombineResults(querySpec *QuerySpec, blockSpecs map[string]*QuerySpec) *QuerySpec {
	numSpecs := len(blockSpecs)
	numProcs := runtime.NumCPU()

	perBlock := numSpecs / numProcs

	if perBlock < 4 {
		return CombineResults(querySpec, blockSpecs)
	}

	allResults := make([]*QuerySpec, 0)
	nextSpecs := make(map[string]*QuerySpec)
	m := &sync.Mutex{}
	var wg sync.WaitGroup

	count := 0

	for k, spec := range blockSpecs {

		nextSpecs[k] = spec
		count += 1

		if count%perBlock == 0 {
			var resultSpec *QuerySpec
			thisSpecs := nextSpecs
			nextSpecs = make(map[string]*QuerySpec)
			wg.Add(1)
			go func() {
				resultSpec = CombineAndPrune(querySpec, thisSpecs)
				m.Lock()
				allResults = append(allResults, resultSpec)
				m.Unlock()
				wg.Done()
			}()
		}
	}

	wg.Wait()

	aggSpecs := make(map[string]*QuerySpec)

	if len(nextSpecs) > 0 {
		aggSpecs["result_last"] = CombineAndPrune(querySpec, nextSpecs)
	}

	for k, v := range allResults {
		aggSpecs[fmt.Sprintf("result_%v", k)] = v
	}

	return CombineResults(querySpec, aggSpecs)

}

func CombineResults(querySpec *QuerySpec, blockSpecs map[string]*QuerySpec) *QuerySpec {

	astart := time.Now()
	resultSpec := *CopyQuerySpec(querySpec)

	masterResult := make(ResultMap)
	masterTimeResult := make(map[int]ResultMap)

	cumulativeResult := querySpec.NewResult()
	cumulativeResult.GroupByKey = "TOTAL"
	if len(querySpec.Groups) > 1 {
		for _, _ = range querySpec.Groups[1:] {
			cumulativeResult.GroupByKey += "\t"
		}
	}

	for _, spec := range blockSpecs {
		masterResult.Combine(&spec.Results)
		resultSpec.MatchedCount += spec.MatchedCount

		for _, result := range spec.Results {
			cumulativeResult.Combine(result)
		}

		for i, v := range spec.TimeResults {
			mval, ok := masterTimeResult[i]

			if !ok {
				masterTimeResult[i] = v
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

	resultSpec.Cumulative = cumulativeResult
	resultSpec.TimeBucket = querySpec.TimeBucket
	resultSpec.TimeResults = masterTimeResult
	resultSpec.Results = masterResult

	aend := time.Now()
	if DEBUG_TIMING {
		Debug("AGGREGATING", len(blockSpecs), "BLOCK RESULTS TOOK", aend.Sub(astart))
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

	for timeBucket, results := range qs.TimeResults {
		interimResult := make(ResultMap)
		for _, res := range results {
			_, ok := qs.Results[res.GroupByKey]
			if ok {
				interimResult[res.GroupByKey] = res
			}
		}

		qs.TimeResults[timeBucket] = interimResult
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
func SearchBlocks(querySpec *QuerySpec, blockList map[string]*TableBlock) map[string]*QuerySpec {
	var wg sync.WaitGroup
	// Each block gets its own querySpec (for locking and combining purposes)
	// after all queries finish executing, the specs are combined
	blockSpecs := make(map[string]*QuerySpec, len(blockList))

	// DONE: why iterate through blocklist after loading it instead of filtering
	// and aggregating while loading them? (and then releasing the blocks)
	// That would mean pushing the call to 'FilterAndAggRecords' to the loading area
	specLock := sync.Mutex{}
	for _, block := range blockList {
		wg.Add(1)
		thisBlock := block
		go func() {
			defer wg.Done()

			blockQuery := CopyQuerySpec(querySpec)

			FilterAndAggRecords(blockQuery, &thisBlock.RecordList)

			specLock.Lock()
			blockSpecs[thisBlock.Name] = blockQuery
			specLock.Unlock()

		}()
	}

	wg.Wait()

	return blockSpecs
}

func (t *Table) MatchAndAggregate(querySpec *QuerySpec) {
	start := time.Now()

	querySpec.Table = t
	blockSpecs := SearchBlocks(querySpec, t.BlockList)
	querySpec.ResetResults()

	// COMBINE THE PER BLOCK RESULTS
	resultSpec := CombineResults(querySpec, blockSpecs)

	aend := time.Now()
	Debug("AGGREGATING TOOK", aend.Sub(start))

	querySpec.Results = resultSpec.Results
	querySpec.TimeResults = resultSpec.TimeResults

	// Aggregating Matched Records
	matched := CombineMatches(blockSpecs)
	if HOLD_MATCHES {
		querySpec.Matched = matched
	}

	end := time.Now()

	querySpec.SortResults(querySpec.OrderBy)

	Debug(string(len(matched)), "RECORDS FILTERED AND AGGREGATED INTO", len(querySpec.Results), "RESULTS, TOOK", end.Sub(start))

}

// vim: foldmethod=marker
