package sybil

var NUM_BUCKETS = 1000
var DEBUG_OUTLIERS = false

// histogram types:
// BasicHist (which gets wrapped in HistCompat to implement the Histogram interface)

type HistogramType string

const (
	HistogramTypeNone  HistogramType = ""
	HistogramTypeBasic HistogramType = "basic"
	HistogramTypeLog   HistogramType = "multi"
)

type HistogramParameters struct {
	Type       HistogramType `json:",omitempty"`
	NumBuckets int           `json:",omitempty"`
	BucketSize int           `json:",omitempty"`
	Weighted   bool          `json:",omitempty"`
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
	GetIntInfo() IntInfo

	Range() (int64, int64)
	StdDev() float64

	NewHist(IntInfo) Histogram
	Combine(Histogram)
}

func NewHist(params HistogramParameters, info IntInfo) Histogram {
	var hist Histogram
	if params.Type == HistogramTypeLog {
		hist = newMultiHist(params, info)
	} else {
		hist = newBasicHist(params, info)
	}
	return hist
}
