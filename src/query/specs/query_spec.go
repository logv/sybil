package sybil

import (
	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/structs"

	md "github.com/logv/sybil/src/lib/metadata"
	hists "github.com/logv/sybil/src/query/hists"
)

const (
	NO_OP       = iota
	OP_AVG      = iota
	OP_HIST     = iota
	OP_DISTINCT = iota
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
}

type savedQueryParams struct {
	Filters      []Filter
	Groups       []Grouping
	Aggregations []Aggregation

	OrderBy    string
	Limit      int16
	TimeBucket int
}

type LuaTable map[string]interface{}

// For outside consumption
type QueryParams savedQueryParams
type QueryResults savedQueryResults

type QuerySpec struct {
	QueryParams
	QueryResults

	// for now, we do not save samples
	// as cached query results.
	// DO NOT CHANGE WITHOUT RUNNING TESTS
	Sorted  []*Result
	Matched RecordList

	BlockList map[string]TableBlock
	Table     *Table

	//	Sessions SessionList

	//	LuaResult LuaTable
	//	LuaState  *C.struct_lua_State
}

type Grouping struct {
	Name   string
	NameID int16
}

type Aggregation struct {
	Op       string
	op_id    int
	Name     string
	NameID   int16
	HistType string
}

type Result struct {
	Hists map[string]hists.Histogram

	GroupByKey  string
	BinaryByKey string
	Count       int64
	Samples     int64
}

func NewResult() *Result {
	added_record := &Result{}
	added_record.Hists = make(map[string]hists.Histogram)
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
			for _, c := range b.Columns {
				if len(c.RCache) > 0 {
					c.RCache = make(map[int]bool)
				}
			}
		}
	}
}
func GroupingForTable(t *Table, name string) Grouping {
	col_id := md.GetTableKeyID(t, name)
	return Grouping{name, col_id}
}

func AggregationForTable(t *Table, name string, op string) Aggregation {
	col_id := md.GetTableKeyID(t, name)

	agg := Aggregation{Name: name, NameID: col_id, Op: op}
	if op == "avg" {
		agg.op_id = OP_AVG
	}

	if op == "hist" {
		agg.op_id = OP_HIST
		agg.HistType = "basic"
		if *FLAGS.LOG_HIST {
			agg.HistType = "multi"

		}

		if *FLAGS.HDR_HIST {
			agg.HistType = "hdr"
		}
	}

	if op == "distinct" {
		agg.op_id = OP_DISTINCT
	}

	_, ok := t.IntInfo[col_id]
	if !ok {
		// TODO: tell our table we need to load all records!
		Debug("MISSING CACHED INFO FOR", agg)
	}
	return agg
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
