package edb

import "sync"

type ResultMap map[string]*Result

type QuerySpec struct {
	Filters      []Filter
	Groups       []Grouping
	Aggregations []Aggregation

	OrderBy string
	Limit   int16
	TimeBucket int

	Results ResultMap
	TimeResults map[int] ResultMap
	Sorted  []*Result
	Matched []*Record

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
	Strs  map[string]string
	Sets  map[string][]string
	Hists map[string]*Hist

	GroupByKey string
	Count      int32
}

func punctuateSpec(querySpec *QuerySpec) {
	querySpec.Results = make(ResultMap)

	querySpec.TimeResults = make(map[int]ResultMap)
	querySpec.c = &sync.Mutex{}
	querySpec.m = &sync.RWMutex{}
	querySpec.r = &sync.RWMutex{}
}
