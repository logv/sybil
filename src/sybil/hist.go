package sybil

var NUM_BUCKETS = 1000
var DEBUG_OUTLIERS = false

// histogram types:
// HDRHist (wrapper around github.com/codahale/hdrhistogram which implements Histogram interface)
// BasicHist (which gets wrapped in HistCompat to implement the Histogram interface)

type Histogram interface {
	Mean() float64
	Max() int64
	Min() int64
	TotalCount() int64

	AddWeightedValue(int64, int64)
	GetPercentiles() []int64
	GetStrBuckets() map[string]int64
	GetIntBuckets() map[int64]int64

	Range() (int64, int64)
	StdDev() float64

	NewHist(*FlagDefs) Histogram
	Combine(interface{})
}

func (t *Table) NewHist(flags *FlagDefs, info *IntInfo) Histogram {
	var hist Histogram
	if *flags.LOG_HIST {
		hist = newMultiHist(flags, t, info)
	} else {
		hist = newBasicHist(flags, t, info)
	}

	return hist
}
