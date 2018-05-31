package sybil

var NUM_BUCKETS = 1000
var DEBUG_OUTLIERS = false

// histogram types:
// HDRHist (wrapper around github.com/codahale/hdrhistogram which implements Histogram interface)
// BasicHist (which gets wrapped in HistCompat to implement the Histogram interface)

type HistogramType string

const (
	HistogramTypeNone  HistogramType = ""
	HistogramTypeBasic HistogramType = "basic"
	HistogramTypeLog   HistogramType = "multi"
	HistogramTypeHDR   HistogramType = "hdr"
)

type HistogramParameters struct {
	Type       HistogramType
	NumBuckets int
	BucketSize int
}

type Histogram interface {
	Mean() float64
	Max() int64
	Min() int64
	TotalCount() int64

	AddWeightedValue(int64, int64)
	GetPercentiles() []int64
	GetStrBuckets() map[string]int64
	GetIntBuckets() map[int64]int64
	IsWeighted() bool

	Range() (int64, int64)
	StdDev() float64

	NewHist(HistogramParameters) Histogram
	Combine(interface{})
}

func (t *Table) NewHist(params HistogramParameters, info *IntInfo, weighted bool) Histogram {
	var hist Histogram
	if params.Type == HistogramTypeLog {
		hist = newMultiHist(params, t, info, weighted)
	} else {
		hist = newBasicHist(params, t, info, weighted)
	}

	return hist
}
