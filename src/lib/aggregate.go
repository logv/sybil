package sybil

import "log"
import "fmt"
import "time"
import "sync"
import "bytes"
import "sort"
import "os"
import "strconv"

import "encoding/binary"

var INTERNAL_RESULT_LIMIT = 100000
var GROUP_BY_WIDTH = 8 // bytes

const (
	NO_OP   = iota
	OP_AVG  = iota
	OP_HIST = iota
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
	if a.Col == SORT_COUNT {
		t1 := a.Results[i].Count
		t2 := a.Results[j].Count

		return t1 > t2
	}

	if *f_OP == "hist" {
		t1 := a.Results[i].Hists[a.Col].Avg
		t2 := a.Results[j].Hists[a.Col].Avg
		return t1 > t2

	}

	t1 := a.Results[i].Ints[a.Col]
	t2 := a.Results[j].Ints[a.Col]
	return t1 > t2
}

func FilterAndAggRecords(querySpec *QuerySpec, recordsPtr *RecordList) int {
	var ok bool
	var binarybuffer []byte = make([]byte, GROUP_BY_WIDTH*len(querySpec.Groups))
	bs := make([]byte, GROUP_BY_WIDTH)
	records := *recordsPtr

	matched_records := 0
	if HOLD_MATCHES {
		querySpec.Matched = make(RecordList, 0)
	}

	var result_map ResultMap
	columns := make(map[int16]*TableColumn)

	if querySpec.TimeBucket <= 0 {
		result_map = querySpec.Results
	}

	var val uint64
	for i := 0; i < len(records); i++ {
		add := true
		r := records[i]

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

		// BUILD GROUPING KEY USING BINARY BYTES
		for i, _ := range binarybuffer {
			binarybuffer[i] = 0
		}

		for i, g := range querySpec.Groups {
			for j, _ := range bs {
				bs[j] = 0
			}

			_, ok := columns[g.name_id]
			if !ok {
				columns[g.name_id] = r.block.GetColumnInfo(g.name_id)
				columns[g.name_id].Type = r.Populated[g.name_id]
			}

			switch r.Populated[g.name_id] {
			case INT_VAL:
				val = uint64(r.Ints[g.name_id])
			case STR_VAL:
				val = uint64(r.Strs[g.name_id])
			}

			binary.LittleEndian.PutUint64(bs, val)
			for j, _ := range bs {
				binarybuffer[i*GROUP_BY_WIDTH+j] = bs[j]
			}
		}

		// IF WE ARE DOING A TIME SERIES AGGREGATION (WHICH CAN BE SLOWER)
		if querySpec.TimeBucket > 0 {
			if len(r.Populated) <= int(TIME_COL_ID) {
				continue
			}

			if r.Populated[TIME_COL_ID] != INT_VAL {
				continue
			}
			val := int64(r.Ints[TIME_COL_ID])

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
				big_record.Count++
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

		added_record.Count++
		count := float64(added_record.Count)
		// GO THROUGH AGGREGATIONS AND REALIZE THEM

		for _, a := range querySpec.Aggregations {
			a_id := a.name_id
			if r.Populated[a_id] == INT_VAL {
				val := int(r.Ints[a_id])

				if a.op_id == OP_AVG {
					// Calculating averages
					partial, ok := added_record.Ints[a.name]
					if !ok {
						partial = 0
					}

					partial = partial + (float64(val)-partial)/float64(count)

					added_record.Ints[a.name] = partial
				}

				if a.op_id == OP_HIST {
					hist, ok := added_record.Hists[a.name]

					if !ok {
						hist = r.block.table.NewHist(r.block.table.get_int_info(a_id))
						added_record.Hists[a.name] = hist
					}
					hist.addValue(val)
				}
			}

		}

	}

	// Now to unpack the byte buffers we oh so stupidly used in the group by...

	if len(querySpec.TimeResults) > 0 {
		for _, result_map := range querySpec.TimeResults {
			translate_group_by(result_map, querySpec.Groups, columns)
		}

	}

	if len(querySpec.Results) > 0 {
		querySpec.Results = *translate_group_by(querySpec.Results, querySpec.Groups, columns)
	}

	return matched_records

}

func translate_group_by(Results ResultMap, Groups []Grouping, columns map[int16]*TableColumn) *ResultMap {

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

	log.Println("JOINING", len(matched), "MATCHED RECORDS TOOK", end.Sub(start))
	return matched

}

func CombineResults(querySpec *QuerySpec, block_specs map[string]*QuerySpec) *QuerySpec {

	astart := time.Now()
	resultSpec := QuerySpec{}
	master_result := make(ResultMap)
	master_time_result := make(map[int]ResultMap)

	for _, spec := range block_specs {
		master_result.Combine(&spec.Results)

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

	resultSpec.TimeBucket = querySpec.TimeBucket
	resultSpec.TimeResults = master_time_result
	resultSpec.Results = master_result

	aend := time.Now()
	log.Println("AGGREGATING", len(block_specs), "BLOCK RESULTS TOOK", aend.Sub(astart))

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
			log.Println("SORTING TOOK", end.Sub(start))
		}

		if len(sorter.Results) > *f_LIMIT {
			sorter.Results = sorter.Results[:*f_LIMIT]
		}

		querySpec.Sorted = sorter.Results
	}

}

func SearchBlocks(querySpec *QuerySpec, block_list map[string]*TableBlock) map[string]*QuerySpec {

	var wg sync.WaitGroup
	// Each block gets its own querySpec (for locking and combining purposes)
	// after all queries finish executing, the specs are combined
	block_specs := make(map[string]*QuerySpec, len(block_list))

	// TODO: why iterate through blocklist after loading it instead of filtering
	// and aggregating while loading them? (and then releasing the blocks)
	// That would mean pushing the call to 'FilterAndAggRecords' to the loading area
	for _, block := range block_list {
		wg.Add(1)
		this_block := block
		go func() {
			defer wg.Done()

			blockQuery := CopyQuerySpec(querySpec)

			FilterAndAggRecords(blockQuery, &this_block.RecordList)

			block_specs[this_block.Name] = blockQuery

			if !*f_JSON {
				fmt.Fprint(os.Stderr, ".")
			}

		}()
	}

	wg.Wait()

	if !*f_JSON {
		fmt.Fprint(os.Stderr, "\n")
	}

	return block_specs
}

func (t *Table) MatchAndAggregate(querySpec *QuerySpec) {
	start := time.Now()

	block_specs := SearchBlocks(querySpec, t.BlockList)

	// COMBINE THE PER BLOCK RESULTS
	resultSpec := CombineResults(querySpec, block_specs)

	aend := time.Now()
	log.Println("AGGREGATING TOOK", aend.Sub(start))

	querySpec.Results = resultSpec.Results
	querySpec.TimeResults = resultSpec.TimeResults

	// Aggregating Matched Records
	matched := CombineMatches(block_specs)
	if HOLD_MATCHES {
		querySpec.Matched = matched
	}

	end := time.Now()

	SortResults(querySpec)

	log.Println(len(matched), "RECORDS FILTERED AND AGGREGATED INTO", len(querySpec.Results), "RESULTS, TOOK", end.Sub(start))

}
