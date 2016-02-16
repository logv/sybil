package sybil

import "log"

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
	Matched     RecordList

	BlockList map[string]TableBlock
	Table     *Table
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
	op_id   int
	name    string
	name_id int16
}

type Result struct {
	Hists map[string]*Hist

	GroupByKey  string
	BinaryByKey string
	Count       int64
	Samples     int64
}

func NewResult() *Result {
	added_record := &Result{}
	added_record.Hists = make(map[string]*Hist)
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
	if next_result == nil {
		return
	}

	if next_result.Count == 0 {
		return
	}

	total_samples := rs.Samples + next_result.Samples
	total_count := rs.Count + next_result.Count

	// combine histograms...
	for k, v := range next_result.Hists {
		_, ok := rs.Hists[k]
		if !ok {
			rs.Hists[k] = v
		} else {
			rs.Hists[k].Combine(v)
		}
	}

	rs.Samples = total_samples
	rs.Count = total_count
}

func (querySpec *QuerySpec) Punctuate() {
	querySpec.Results = make(ResultMap)
	querySpec.TimeResults = make(map[int]ResultMap)
}

func (t *Table) Grouping(name string) Grouping {
	col_id := t.get_key_id(name)
	return Grouping{name, col_id}
}

func (t *Table) Aggregation(name string, op string) Aggregation {
	col_id := t.get_key_id(name)
	agg := Aggregation{name: name, name_id: col_id, op: op}
	if op == "avg" {
		agg.op_id = OP_AVG
	}

	if op == "hist" {
		agg.op_id = OP_HIST
	}

	log.Println("AGG", op, agg.op_id)
	return agg
}
