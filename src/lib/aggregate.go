package pcs

import "bytes"
import "log"
import "fmt"
import "time"
import "sync"
import "sync/atomic"
import "sort"

var INTERNAL_RESULT_LIMIT = 100000

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

func FilterAndAggRecords(querySpec *QuerySpec, recordsPtr *[]*Record) []*Record {
	var buffer bytes.Buffer
	records := *recordsPtr

	ret := make([]*Record, 0)

	var result_map ResultMap
	if querySpec.TimeBucket <= 0 {
		result_map = querySpec.Results
	}

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

		if add {
			ret = append(ret, r)
		} else {
			// if we aren't adding this record... then we shouldn't continue looking at it
			continue
		}

		// BELOW HERE IS THE AGGREGATION CORE
		if len(querySpec.Groups) == 0 {
			buffer.WriteString("total")
		}

		// BUILD GROUPING KEY
		for _, g := range querySpec.Groups {
			if r.Populated[g.name_id] == 0 {
				buffer.WriteRune(':')
				continue
			}
			col_id := g.name_id
			col := r.block.getColumnInfo(col_id)
			val := col.get_string_for_val(int32(r.Strs[col_id]))
			buffer.WriteString(string(val))
			buffer.WriteRune(':')
		}

		// IF WE ARE DOING A TIME SERIES AGGREGATION (WHICH CAN BE SLOWER)
		if querySpec.TimeBucket > 0 {
			val, ok := r.getIntVal(*f_TIME_COL)
			if ok {
				val = int(val/querySpec.TimeBucket) * querySpec.TimeBucket
				result_map, ok = querySpec.TimeResults[val]

				if !ok {
					result_map = make(ResultMap)
					existing, ok := querySpec.TimeResults[val]
					if !ok {
						querySpec.TimeResults[val] = result_map
					} else {
						result_map = existing
					}
				}
			} else {
				continue
			}

		}

		group_key := buffer.String()
		buffer.Reset()

		added_record, ok := result_map[group_key]

		// BUILD GROUPING RECORD
		if !ok {
			length := len(result_map)

			// TODO: take into account whether we are doint time series or not...
			if length >= INTERNAL_RESULT_LIMIT {
				continue
			}

			added_record = NewResult()
			added_record.GroupByKey = group_key

			// WARNING: this is an annoying thread barrier that happens.
			// TODO: replace it with a RW mutex instead of just R mutex
			existing_record, ok := result_map[group_key]

			if !ok {
				// Even though we are about to lock, someone else might have inserted
				// right before we grabbed the lock...
				existing_record, ok = result_map[group_key]
				if ok {
					added_record = existing_record
				} else {
					result_map[group_key] = added_record
				}
			} else {
				added_record = existing_record
			}
		}

		count := atomic.AddInt32(&added_record.Count, 1)
		// GO THROUGH AGGREGATIONS AND REALIZE THEM

		for _, a := range querySpec.Aggregations {
			a_id := a.name_id
			if r.Populated[a_id] == INT_VAL {
				val := int(r.Ints[a_id])

				if a.op == "avg" {
					// Calculating averages
					partial, ok := added_record.Ints[a.name]
					if !ok {
						partial = 0
					}

					partial = partial + (float64(val)-partial)/float64(count)

					added_record.Ints[a.name] = partial
				}

				if a.op == "hist" {
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

	return ret[:]
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

func CombineMatches(block_specs map[string]*QuerySpec) []*Record {
	start := time.Now()
	matched := make([]*Record, 0)
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

			ret := FilterAndAggRecords(blockQuery, &this_block.RecordList)
			blockQuery.Matched = ret
			block_specs[this_block.Name] = blockQuery

			if !*f_JSON {
				fmt.Print(".")
			}

		}()
	}

	wg.Wait()

	if !*f_JSON {
		fmt.Print("\n")
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
	querySpec.Matched = matched

	end := time.Now()

	SortResults(querySpec)

	log.Println(len(querySpec.Matched), "RECORDS FILTERED AND AGGREGATED INTO", len(querySpec.Results), "RESULTS, TOOK", end.Sub(start))

}
