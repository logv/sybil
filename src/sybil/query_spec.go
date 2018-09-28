package sybil

import (
	"crypto/md5"
	"encoding/json"
	"fmt"

	hll "github.com/logv/loglogbeta"
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
	Filters      []Filter              `json:",omitempty"`
	Groups       []Grouping            `json:",omitempty"`
	Aggregations []Aggregation         `json:",omitempty"`
	Distincts    []Grouping            `json:",omitempty"` // list of columns we are creating a count distinct query on
	StrReplace   map[string]StrReplace `json:",omitempty"`

	OrderBy    string `json:",omitempty"`
	PruneBy    string `json:",omitempty"`
	Limit      int    `json:",omitempty"`
	TimeBucket int    `json:",omitempty"`

	Samples       bool `json:",omitempty"`
	CachedQueries bool `json:",omitempty"`

	HistogramParameters HistogramParameters `json:",omitempty"`
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
	nameID int16
}

type Aggregation struct {
	Op     Op
	Name   string
	nameID int16
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

	nh := h.NewHist(info)

	for bucket, count := range h.GetIntBuckets() {
		nh.AddWeightedValue(bucket, count)
	}

	for bucket, count := range ph.GetIntBuckets() {
		nh.AddWeightedValue(bucket, count)
	}
	//spew.Dump(nh)

	return nh
}

// fastMergeable indicates if two histograms can be merged on the fast path vs a full merge.
func fastMergeable(l, r Histogram) bool {
	l1, r1 := l.Range()
	l2, r2 := r.Range()
	pl, pr := l.GetParameters(), r.GetParameters()
	bl, br := pl.BucketSize, pr.BucketSize
	nl, nr := pl.NumBuckets, pr.NumBuckets
	return l1 == l2 && r1 == r2 && bl == br && nl == nr
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
		ph, ok := rs.Hists[k]

		if !ok {
			nh := h.NewHist(h.GetIntInfo())

			if FLAGS.MERGE_TABLE != nil {
				switch v := nh.(type) {
				case *HistCompat:
					v.TrackPercentiles()
				case *MultiHistCompat:
					v.TrackPercentiles()
				default:
					Warn("Unknown Hist Type during aggregation phase")
				}

			}

			nh.Combine(h)
			rs.Hists[k] = nh
			continue
		}

		// If we are doing a node aggregation, we need to go the slow route
		// when merging histograms because we can't be sure they were created
		// with the same extents (being that they may originate from different
		// nodes)
		if fastMergeable(h, ph) && FLAGS.MERGE_TABLE == nil {
			rs.Hists[k].Combine(h)
		} else {
			rs.Hists[k] = fullMergeHist(h, ph)
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

// Punctuate resets results on the QuerySpec.
func (qs *QuerySpec) Punctuate() {
	qs.Results = make(ResultMap)
	qs.TimeResults = make(map[int]ResultMap)
}

// ResetResults resets results on the QuerySpec and clears table caches.
func (qs *QuerySpec) ResetResults() {
	qs.Punctuate()

	if qs.Table != nil && qs.Table.BlockList != nil {
		// Reach into all our table blocks and reset their REGEX CACHE
		for _, b := range qs.Table.BlockList {
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

func (t *Table) Aggregation(name string, op Op) Aggregation {
	colID := t.getKeyID(name)

	agg := Aggregation{Name: name, nameID: colID, Op: op}

	_, ok := t.IntInfo[colID]
	if !ok {
		// TODO: tell our table we need to load all records!
		Debug("MISSING CACHED INFO FOR", agg)
	}
	return agg
}

// cacheKey returns a stable identifier.
func (qp QueryParams) cacheKey() string {
	buf, err := json.Marshal(qp)
	if err != nil {
		panic(err)
	}

	h := md5.New()
	h.Write(buf)

	ret := fmt.Sprintf("%x", h.Sum(nil))
	return ret

}
