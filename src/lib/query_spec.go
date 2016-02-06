package pcs

import "sync"
import "sort"
import "log"
import "fmt"
import "encoding/json"
import "os"

type ResultMap map[string]*Result

type QuerySpec struct {
	Filters      []Filter
	Groups       []Grouping
	Aggregations []Aggregation

	OrderBy    string
	Limit      int16
	TimeBucket int

	Results     ResultMap
	TimeResults map[int]ResultMap
	Sorted      []*Result
	Matched     []*Record

	BlockList map[string]TableBlock

	m *sync.RWMutex
	r *sync.RWMutex
	c *sync.Mutex
}

type Filter interface {
	Filter(*Record) bool
}

type Grouping struct {
	name    string
	name_id int16
}

type Aggregation struct {
	op      string
	name    string
	name_id int16
}

type Result struct {
	Ints  map[string]float64
	Hists map[string]*Hist

	GroupByKey string
	Count      int32
}

func NewResult() *Result {
	added_record := &Result{}
	added_record.Hists = make(map[string]*Hist)
	added_record.Ints = make(map[string]float64)
	added_record.Count = 0
	return added_record
}

func (master_result *ResultMap) Combine(results *ResultMap) {
	for k, v := range *results {
		mval, ok := (*master_result)[k]
		if !ok {
			(*master_result)[k] = v
		} else {
			mval.Combine(v)
		}
	}
}

// This does an in place combine of the next_result into this one...
func (rs *Result) Combine(next_result *Result) {
	total_count := rs.Count + next_result.Count
	next_ratio := float64(next_result.Count) / float64(total_count)
	this_ratio := float64(rs.Count) / float64(total_count)

	// Combine averages first...
	for k, v := range next_result.Ints {
		mval, ok := rs.Ints[k]
		if !ok {
			rs.Ints[k] = v
		} else {
			rs.Ints[k] = (v * next_ratio) + (mval * this_ratio)
		}

	}

	// combine histograms...
	for k, v := range next_result.Hists {
		_, ok := rs.Hists[k]
		if !ok {
			rs.Hists[k] = v
		} else {
			rs.Hists[k].Combine(v)
		}
	}

	rs.Count = total_count
}

func (querySpec *QuerySpec) Punctuate() {
	querySpec.Results = make(ResultMap)
	querySpec.TimeResults = make(map[int]ResultMap)

	querySpec.c = &sync.Mutex{}
	querySpec.m = &sync.RWMutex{}
	querySpec.r = &sync.RWMutex{}
}

func (t *Table) Grouping(name string) Grouping {
	col_id := t.get_key_id(name)
	return Grouping{name, col_id}
}

func (t *Table) Aggregation(name string, op string) Aggregation {
	col_id := t.get_key_id(name)
	return Aggregation{name: name, name_id: col_id, op: op}
}

func printTimeResults(querySpec *QuerySpec) {
	keys := make([]int, 0)

	for k, _ := range querySpec.TimeResults {
		keys = append(keys, k)
	}

	sort.Sort(ByVal(keys))

	for _, k := range keys {
		fmt.Println("BUCKET", k, len(querySpec.TimeResults[k]))
	}

}

func printSortedResults(querySpec *QuerySpec) {
	sorted := querySpec.Sorted
	if int(querySpec.Limit) < len(querySpec.Sorted) {
		sorted = querySpec.Sorted[:querySpec.Limit]
	}
	for _, v := range sorted {
		printResult(querySpec, v)
	}
}

func printResult(querySpec *QuerySpec, v *Result) {
	fmt.Println(fmt.Sprintf("%-20s", v.GroupByKey)[:20], fmt.Sprintf("%.0d", v.Count))
	for _, agg := range querySpec.Aggregations {
		col_name := fmt.Sprintf("  %5s", agg.name)
		if *f_OP == "hist" {
			h, ok := v.Hists[agg.name]
			if !ok {
				log.Println("NO HIST AROUND FOR KEY", agg.name, v.GroupByKey)
				continue
			}
			p := h.getPercentiles()

			if len(p) > 0 {
				avg_str := fmt.Sprintf("%.2f", h.Avg)
				fmt.Println(col_name, avg_str, p[0], p[25], p[50], p[75], p[99])
			} else {
				fmt.Println(col_name, "No Data")
			}
		} else if *f_OP == "avg" {
			fmt.Println(col_name, fmt.Sprintf("%.2f", v.Ints[agg.name]))
		}
	}
}

func printResults(querySpec *QuerySpec) {
	if querySpec.TimeBucket > 0 {
		printTimeResults(querySpec)

		return
	}

	count := 0
	for _, v := range querySpec.Results {
		printResult(querySpec, v)
		count++
		if count >= int(querySpec.Limit) {
			return
		}
	}
}

func (qs *QuerySpec) printResults() {
	if *f_JSON {
		b, err := json.Marshal(qs.Results)
		if err == nil {
			os.Stdout.Write(b)
		} else {
			log.Fatal("JSON encoding error", err)
		}

		return
	}
	if *f_PRINT {
		log.Println("PRINTING RESULTS")

		if qs.OrderBy != "" {
			printSortedResults(qs)
		} else {
			printResults(qs)
		}
	}
}
