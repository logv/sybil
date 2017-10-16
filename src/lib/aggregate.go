package sybil

import "time"
import "bytes"
import "sort"
import "strconv"
import "sync"
import "math"

import "encoding/binary"

var InternalResultLimit = 100000
var GroupByWidth = 8 // bytes

const (
	NoOp       = iota
	OpAvg      = iota
	OpHist     = iota
	OpDistinct = iota
)

var GroupDelimiter = "\t"

type SortResultsByCol struct {
	Results []*Result

	Col string
}

func (a SortResultsByCol) Len() int      { return len(a.Results) }
func (a SortResultsByCol) Swap(i, j int) { a.Results[i], a.Results[j] = a.Results[j], a.Results[i] }

// This sorts the records in descending order
func (a SortResultsByCol) Less(i, j int) bool {
	if a.Col == OPTS.SortCount {
		t1 := a.Results[i].Count
		t2 := a.Results[j].Count

		return t1 > t2
	}

	if *FLAGS.Op == "hist" {
		t1 := a.Results[i].Hists[a.Col].Mean()
		t2 := a.Results[j].Hists[a.Col].Mean()
		return t1 > t2

	}

	t1 := a.Results[i].Hists[a.Col].Mean()
	t2 := a.Results[j].Hists[a.Col].Mean()
	return t1 > t2
}

func FilterAndAggRecords(querySpec *QuerySpec, recordsPtr *RecordList) int {
	var ok bool
	var binarybuffer []byte = make([]byte, GroupByWidth*len(querySpec.Groups))

	bs := make([]byte, GroupByWidth)
	zero := make([]byte, GroupByWidth)
	records := *recordsPtr

	var weight = int64(1)

	matchedRecords := 0
	if HoldMatches {
		querySpec.Matched = make(RecordList, 0)
	}

	var resultMap ResultMap
	length := len(querySpec.Table.KeyTable)
	columns := make([]*TableColumn, length)

	if querySpec.TimeBucket <= 0 {
		resultMap = querySpec.Results
	}

	for i := 0; i < len(records); i++ {
		add := true
		r := records[i]

		if OPTS.WeightCol && r.Populated[OPTS.WeightColID] == IntVal {
			weight = int64(r.Ints[OPTS.WeightColID])
		}

		// FILTERING
		for j := 0; j < len(querySpec.Filters); j++ {
			// returns True if the record matches!
			ret := !querySpec.Filters[j].Filter(r)
			if ret {
				add = false
				break
			}
		}

		if !add {
			continue
		}

		matchedRecords++
		if HoldMatches {
			querySpec.Matched = append(querySpec.Matched, r)
		}

		if *FLAGS.LUA {
			continue
		}

		for i, g := range querySpec.Groups {
			copy(bs, zero)

			if columns[g.nameID] == nil && r.Populated[g.nameID] != _NoVal {
				columns[g.nameID] = r.block.GetColumnInfo(g.nameID)
				columns[g.nameID].Type = r.Populated[g.nameID]
			}

			switch r.Populated[g.nameID] {
			case IntVal:
				binary.LittleEndian.PutUint64(bs, uint64(r.Ints[g.nameID]))
			case StrVal:
				binary.LittleEndian.PutUint64(bs, uint64(r.Strs[g.nameID]))
			case _NoVal:
				binary.LittleEndian.PutUint64(bs, math.MaxUint64)
			}

			copy(binarybuffer[i*GroupByWidth:], bs)
		}

		// IF WE ARE DOING A TIME SERIES AGGREGATION (WHICH CAN BE SLOWER)
		if querySpec.TimeBucket > 0 {
			if len(r.Populated) <= int(OPTS.TimeColID) {
				continue
			}

			if r.Populated[OPTS.TimeColID] != IntVal {
				continue
			}
			val := int64(r.Ints[OPTS.TimeColID])

			bigRecord, bOk := querySpec.Results[string(binarybuffer)]
			if !bOk {
				if len(querySpec.Results) < InternalResultLimit {
					bigRecord = NewResult()
					bigRecord.BinaryByKey = string(binarybuffer)
					querySpec.Results[string(binarybuffer)] = bigRecord
					bOk = true
				}
			}

			if bOk {
				bigRecord.Samples++
				bigRecord.Count += weight
			}

			val = int64(int(val) / querySpec.TimeBucket * querySpec.TimeBucket)
			resultMap, ok = querySpec.TimeResults[int(val)]

			if !ok {
				// TODO: this make call is kind of slow...
				resultMap = make(ResultMap)
				querySpec.TimeResults[int(val)] = resultMap
			}

		}

		addedRecord, ok := resultMap[string(binarybuffer)]

		// BUILD GROUPING RECORD
		if !ok {
			// TODO: take into account whether we are doint time series or not...
			if len(resultMap) >= InternalResultLimit {
				continue
			}

			addedRecord = NewResult()
			addedRecord.BinaryByKey = string(binarybuffer)

			resultMap[string(binarybuffer)] = addedRecord
		}

		addedRecord.Samples++
		addedRecord.Count += weight

		// GO THROUGH AGGREGATIONS AND REALIZE THEM
		for _, a := range querySpec.Aggregations {
			switch r.Populated[a.nameID] {
			case IntVal:
				val := int64(r.Ints[a.nameID])

				hist, ok := addedRecord.Hists[a.Name]

				if !ok {
					hist = r.block.table.NewHist(r.block.table.getIntInfo(a.nameID))
					addedRecord.Hists[a.Name] = hist
				}

				hist.RecordValues(val, weight)
			}

		}

	}

	// Now to unpack the byte buffers we oh so stupidly used in the group by...

	if len(querySpec.TimeResults) > 0 {
		for k, resultMap := range querySpec.TimeResults {
			querySpec.TimeResults[k] = *translateGroupBy(resultMap, querySpec.Groups, columns)
		}

	}

	if len(querySpec.Results) > 0 {
		querySpec.Results = *translateGroupBy(querySpec.Results, querySpec.Groups, columns)
	}

	if *FLAGS.LUA {
		querySpec.luaInit()
		querySpec.luaMap(&querySpec.Matched)
	}

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
			bs = []byte(r.BinaryByKey[i*GroupByWidth : (i+1)*GroupByWidth])

			col := columns[g.nameID]

			if col == nil {
				buffer.WriteString(GroupDelimiter)
				continue
			}

			val := binary.LittleEndian.Uint64(bs)
			switch col.Type {
			case IntVal:
				buffer.WriteString(strconv.FormatInt(int64(val), 10))
			case StrVal:
				buffer.WriteString(col.getStringForVal(int32(val)))

			}

			buffer.WriteString(GroupDelimiter)

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

func CombineResults(querySpec *QuerySpec, blockSpecs map[string]*QuerySpec) *QuerySpec {

	astart := time.Now()
	resultSpec := QuerySpec{}
	resultSpec.Table = querySpec.Table
	resultSpec.LuaResult = make(LuaTable)

	if *FLAGS.LUA {
		resultSpec.luaInit()
	}

	masterResult := make(ResultMap)
	masterTimeResult := make(map[int]ResultMap)

	cumulativeResult := NewResult()
	cumulativeResult.GroupByKey = "TOTAL"
	if len(querySpec.Groups) > 1 {
		for range querySpec.Groups[1:] {
			cumulativeResult.GroupByKey += "\t"
		}
	}

	for _, spec := range blockSpecs {
		masterResult.Combine(&spec.Results)

		if *FLAGS.LUA {
			resultSpec.luaCombine(spec)
		}

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

	if *FLAGS.LUA {
		resultSpec.luaFinalize()
	}

	aend := time.Now()
	Debug("AGGREGATING", len(blockSpecs), "BLOCK RESULTS TOOK", aend.Sub(astart))

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
		if DebugTiming {
			Debug("SORTING TOOK", end.Sub(start))
		}

		if len(sorter.Results) > *FLAGS.Limit {
			sorter.Results = sorter.Results[:*FLAGS.Limit]
		}

		querySpec.Sorted = sorter.Results
	}

}

// OLD SEARCHING FUNCTIONS BELOW HERE
func SearchBlocks(querySpec *QuerySpec, blockList map[string]*TableBlock) map[string]*QuerySpec {
	var wg sync.WaitGroup
	// Each block gets its own querySpec (for locking and combining purposes)
	// after all queries finish executing, the specs are combined
	blockSpecs := make(map[string]*QuerySpec, len(blockList))

	// TODO: why iterate through blocklist after loading it instead of filtering
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
	if HoldMatches {
		querySpec.Matched = matched
	}

	end := time.Now()

	SortResults(querySpec)

	Debug(string(len(matched)), "RECORDS FILTERED AND AGGREGATED INTO", len(querySpec.Results), "RESULTS, TOOK", end.Sub(start))

}
