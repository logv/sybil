package sybil

import "C"

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
	Name   string
	nameID int16
}

type Aggregation struct {
	Op       string
	opID     int
	Name     string
	nameID   int16
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
	addedRecord := &Result{}
	addedRecord.Hists = make(map[string]Histogram)
	addedRecord.Count = 0
	return addedRecord
}

func (masterResult *ResultMap) Combine(results *ResultMap) {
	for k, v := range *results {
		mval, ok := (*masterResult)[k]
		if !ok {
			(*masterResult)[k] = v
		} else {
			mval.Combine(v)
		}
	}
}

// This does an in place combine of the nextResult into this one...
func (rs *Result) Combine(nextResult *Result) {
	if nextResult == nil {
		return
	}

	if nextResult.Count == 0 {
		return
	}

	totalSamples := rs.Samples + nextResult.Samples
	totalCount := rs.Count + nextResult.Count

	// combine histograms...
	for k, h := range nextResult.Hists {
		_, ok := rs.Hists[k]
		if !ok {
			nh := h.NewHist()

			nh.Combine(h)
			rs.Hists[k] = nh
		} else {
			rs.Hists[k].Combine(h)
		}
	}

	rs.Samples = totalSamples
	rs.Count = totalCount
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
	colID := t.getKeyID(name)
	return Grouping{name, colID}
}

func (t *Table) Aggregation(name string, op string) Aggregation {
	colID := t.getKeyID(name)

	agg := Aggregation{Name: name, nameID: colID, Op: op}
	if op == "avg" {
		agg.opID = OpAvg
	}

	if op == "hist" {
		agg.opID = OpHist
		agg.HistType = "basic"
		if *FLAGS.LogHist {
			agg.HistType = "multi"

		}

		if *FLAGS.HdrHist {
			agg.HistType = "hdr"
		}
	}

	if op == "distinct" {
		agg.opID = OpDistinct
	}

	_, ok := t.IntInfo[colID]
	if !ok {
		// TODO: tell our table we need to load all records!
		Debug("MISSING CACHED INFO FOR", agg)
	}
	return agg
}
