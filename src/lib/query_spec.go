package sybil

import "C"
import (
	"github.com/logv/sybil/src/lib/common"
)

type ResultMap map[string]*Result

// This info gets cached when we use
// the query cache. anything in the main
// QuerySpec will not get cached
type savedQueryResults struct {
	Cumulative   *Result
	Results      ResultMap
	TimeResults  map[int]ResultMap
	MatchedCount int
	Sorted       []*Result
	Matched      RecordList
}

type savedQueryParams struct {
	Filters      []Filter
	Groups       []Grouping
	Aggregations []Aggregation

	OrderBy    string
	Limit      int16
	TimeBucket int
}

// For outside consumption
type QueryParams savedQueryParams
type QueryResults savedQueryResults

type QuerySpec struct {
	QueryParams
	QueryResults

	BlockList map[string]TableBlock
	Table     *Table

	Sessions SessionList

	LuaResult LuaTable
	LuaState  *C.struct_lua_State
}

type Filter interface {
	Filter(*Record) bool
}

type Grouping struct {
	Name    string
	name_id int16
}

type Aggregation struct {
	Op       string
	op_id    int
	Name     string
	name_id  int16
	HistType string
}

type Result struct {
	Hists map[string]Histogram

	GroupByKey  string
	BinaryByKey string
	Count       int64
	Samples     int64
}

func NewResult() *Result {
	added_record := &Result{}
	added_record.Hists = make(map[string]Histogram)
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
	for k, h := range next_result.Hists {
		_, ok := rs.Hists[k]
		if !ok {
			nh := h.NewHist()

			nh.Combine(h)
			rs.Hists[k] = nh
		} else {
			rs.Hists[k].Combine(h)
		}
	}

	rs.Samples = total_samples
	rs.Count = total_count
}

func (querySpec *QuerySpec) Punctuate() {
	querySpec.Results = make(ResultMap)
	querySpec.TimeResults = make(map[int]ResultMap)
}

func (querySpec *QuerySpec) ResetResults() {
	querySpec.Punctuate()

	if querySpec.Table != nil && querySpec.Table.BlockList != nil {
		// Reach into all our table blocks and reset their REGEX CACHE
		for _, b := range querySpec.Table.BlockList {
			for _, c := range b.columns {
				if len(c.RCache) > 0 {
					c.RCache = make(map[int]bool)
				}
			}
		}
	}
}
func (t *Table) Grouping(name string) Grouping {
	col_id := t.get_key_id(name)
	return Grouping{name, col_id}
}

func (t *Table) Aggregation(name string, op string) Aggregation {
	col_id := t.get_key_id(name)

	agg := Aggregation{Name: name, name_id: col_id, Op: op}
	if op == "avg" {
		agg.op_id = OP_AVG
	}

	if op == "hist" {
		agg.op_id = OP_HIST
		agg.HistType = "basic"
		if *common.FLAGS.LOG_HIST {
			agg.HistType = "multi"

		}

		if *common.FLAGS.HDR_HIST {
			agg.HistType = "hdr"
		}
	}

	if op == "distinct" {
		agg.op_id = OP_DISTINCT
	}

	_, ok := t.IntInfo[col_id]
	if !ok {
		// TODO: tell our table we need to load all records!
		common.Debug("MISSING CACHED INFO FOR", agg)
	}
	return agg
}
