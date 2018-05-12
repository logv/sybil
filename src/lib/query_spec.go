package sybil

import hll "github.com/logv/loglogbeta"

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
	Distincts    []Grouping // list of columns we are creating a count distinct query on
	StrReplace   map[string]StrReplace

	OrderBy    string
	PruneBy    string
	Limit      int16
	TimeBucket int
}

func Min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

// For outside consumption
type QueryParams savedQueryParams
type QueryResults savedQueryResults

type QuerySpec struct {
	QueryParams
	QueryResults

	BlockList map[string]TableBlock
	Table     *Table
}

type Filter interface {
	Filter(*Record) bool
}

type Grouping struct {
	Name   string
	nameId int16
}

type Aggregation struct {
	Op       string
	opId     int
	Name     string
	nameId   int16
	HistType string
}

type Result struct {
	Hists    map[string]Histogram
	Distinct *hll.LogLogBeta

	GroupByKey  string
	BinaryByKey string
	Count       int64
	Samples     int64
}

func (qs *QuerySpec) NewResult() *Result {
	addedRecord := &Result{}
	addedRecord.Hists = make(map[string]Histogram)

	if len(qs.Distincts) > 0 {
		addedRecord.Distinct = hll.New()
	}

	addedRecord.Count = 0
	return addedRecord
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

func fullMergeHist(h, ph Histogram) Histogram {
	l1, r1 := h.Range()
	l2, r2 := ph.Range()

	info := IntInfo{Min: Min(l1, l2), Max: Max(r1, r2)}

	nh := OPTS.MERGE_TABLE.NewHist(&info)

	for bucket, count := range h.GetIntBuckets() {
		nh.AddWeightedValue(bucket, count)
	}

	for bucket, count := range ph.GetIntBuckets() {
		nh.AddWeightedValue(bucket, count)
	}

	return nh
}

// This does an in place combine of the next_result into this one...
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

		// If we are doing a node aggregation, we have a MERGE_TABLE
		// set, which means we should go the slow route when merging
		// histograms because we can't be sure they were created with
		// the same extents (being that they may originate from different
		// nodes)
		if OPTS.MERGE_TABLE != nil {
			ph, ok := rs.Hists[k]

			if ok {
				rs.Hists[k] = fullMergeHist(h, ph)
			} else {
				rs.Hists[k] = h
			}

		} else {
			_, ok := rs.Hists[k]
			if !ok {
				nh := h.NewHist()

				nh.Combine(h)
				rs.Hists[k] = nh
			} else {
				rs.Hists[k].Combine(h)
			}
		}
	}

	// combine count distincts
	if nextResult.Distinct != nil {

		if rs.Distinct == nil {
			rs.Distinct = nextResult.Distinct
		} else {
			rs.Distinct.Merge(nextResult.Distinct)

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
	colId := t.getKeyId(name)
	return Grouping{name, colId}
}

func (t *Table) Aggregation(name string, op string) Aggregation {
	colId := t.getKeyId(name)

	agg := Aggregation{Name: name, nameId: colId, Op: op}
	if op == "avg" {
		agg.opId = OP_AVG
	}

	if op == "hist" {
		agg.opId = OP_HIST
		agg.HistType = "basic"
		if *FLAGS.LOG_HIST {
			agg.HistType = "multi"

		}

		if *FLAGS.HDR_HIST {
			agg.HistType = "hdr"
		}
	}

	if op == DISTINCT_STR {
		agg.opId = OP_DISTINCT
	}

	_, ok := t.IntInfo[colId]
	if !ok {
		// TODO: tell our table we need to load all records!
		Debug("MISSING CACHED INFO FOR", agg)
	}
	return agg
}
