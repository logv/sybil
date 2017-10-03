package hists

import "github.com/logv/sybil/src/lib/config"
import . "github.com/logv/sybil/src/lib/structs"

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

	RecordValues(int64, int64) error
	GetPercentiles() []int64
	GetBuckets() map[string]int64
	StdDev() float64
	NewHist() Histogram

	Combine(interface{})
}

func NewHist(t *Table, info *IntInfo) Histogram {
	var hist Histogram
	if *config.FLAGS.HDR_HIST && ENABLE_HDR {
		hist = newHDRHist(t, info)
	} else if *config.FLAGS.LOG_HIST {
		hist = NewMultiHist(t, info)
	} else {
		hist = NewBasicHist(t, info)
	}

	return hist
}
