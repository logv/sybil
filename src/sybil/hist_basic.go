package sybil

import (
	"log"
	"math"
	"sort"
	"strconv"
)

// {{{ BASIC HIST

type BasicHistCachedInfo struct {
	HistogramParameters
	Values         []int64
	Averages       []float64
	PercentileMode bool

	Outliers   []int64
	Underliers []int64

	Max     int64
	Min     int64
	Samples int
	Count   int64
	Avg     float64
	Info    IntInfo
}

// TODO: consider elimination
type BasicHist struct {
	BasicHistCachedInfo
}

func (h *BasicHist) SetupBuckets() {
	// set up initial variables for max and min to be extrema in other
	// direction
	h.Avg = 0
	h.Count = 0
	h.Min = h.Info.Min
	h.Max = h.Info.Max

	if h.PercentileMode {

		h.Outliers = make([]int64, 0)
		h.Underliers = make([]int64, 0)

		size := int64(h.Max - h.Min)
		if h.NumBuckets == 0 {
			h.NumBuckets = NUM_BUCKETS
		}
		h.BucketSize = int(size / int64(h.NumBuckets))

		if h.BucketSize == 0 {
			if size < 100 {
				h.BucketSize = 1
				h.NumBuckets = int(size)
			} else {
				h.BucketSize = int(size / int64(100))
				h.NumBuckets = int(size / int64(h.BucketSize))
			}
		}

		h.NumBuckets++

		h.Values = make([]int64, h.NumBuckets+1)
		h.Averages = make([]float64, h.NumBuckets+1)
	}
}

func newBasicHist(params HistogramParameters, info *IntInfo) *HistCompat {

	basicHist := BasicHist{}
	compatHist := HistCompat{&basicHist}
	compatHist.HistogramParameters = params
	compatHist.Info = *info

	if params.Type != HistogramTypeNone {
		compatHist.TrackPercentiles()
	}

	return &compatHist

}

// TrackPercentiles sets up the percentile buckets.
func (h *BasicHist) TrackPercentiles() {
	h.PercentileMode = true

	h.SetupBuckets()
}

func (h *BasicHist) AddValue(value int64) {
	h.AddWeightedValue(value, 1)
}

func (h *BasicHist) Sum() int64 {
	return int64(h.Avg * float64(h.Count))
}

func (h *BasicHist) AddWeightedValue(value int64, weight int64) {
	// TODO: use more appropriate discard method for .Min to express an order of
	// magnitude
	if value > h.Info.Max*10 || value < h.Info.Min {
		if DEBUG_OUTLIERS {
			log.Println("IGNORING OUTLIER VALUE", value, "MIN IS", h.Info.Min, "MAX IS", h.Info.Max)
		}
		return
	}

	if h.Weighted || weight > 1 {
		h.Samples++
		h.Count += weight
	} else {
		h.Count++
	}

	h.Avg = h.Avg + ((float64(value)-h.Avg)/float64(h.Count))*float64(weight)

	if value > h.Max {
		h.Max = value
	}

	if value < h.Min {
		h.Min = value
	}

	if !h.PercentileMode {
		return
	}

	bucketValue := (value - h.Min) / int64(h.BucketSize)

	if bucketValue >= int64(len(h.Values)) {
		h.Outliers = append(h.Outliers, value)
		bucketValue = int64(len(h.Values) - 1)
	}

	if bucketValue < 0 {
		h.Underliers = append(h.Underliers, value)
		bucketValue = 0
	}

	partial := h.Averages[bucketValue]

	// update counts
	h.Values[bucketValue] += weight

	// update bucket averages
	h.Averages[bucketValue] = partial + ((float64(value) - partial) / float64(h.Values[bucketValue]) * float64(weight))
}

func (h *BasicHist) GetPercentiles() []int64 {
	if h.Count == 0 {
		return make([]int64, 0)
	}

	percentiles := make([]int64, 101)
	keys := make([]int, 0)

	// unpack the bucket values!
	for k := range h.Values {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	percentiles[0] = h.Min
	count := int64(0)
	prevP := int64(0)
	for _, k := range keys {
		keyCount := h.Values[k]
		count = count + keyCount
		p := (100 * count) / h.Count
		for ip := prevP; ip <= p; ip++ {
			percentiles[ip] = (int64(k) * int64(h.BucketSize)) + h.Min

		}
		percentiles[p] = int64(k)
		prevP = p
	}

	return percentiles[:100]
}

// VARIANCE is defined as the squared error from the mean
func (h *BasicHist) GetVariance() float64 {
	std := h.GetStdDev()
	return std * std
}

// STD DEV is defined as sqrt(VARIANCE)
func (h *BasicHist) GetStdDev() float64 {
	// TOTAL VALUES

	sumVariance := float64(0)
	for bucket, count := range h.Values {
		val := int64(bucket)*int64(h.BucketSize) + h.Min
		delta := float64(val) - h.Avg

		ratio := float64(count) / float64(h.Count)

		// unbiased variance. probably unstable
		sumVariance += (float64(delta*delta) * ratio)
	}

	for _, val := range h.Outliers {
		delta := math.Pow(float64(val)-h.Avg, 2)
		ratio := 1 / float64(h.Count)
		sumVariance += (float64(delta) * ratio)
	}

	for _, val := range h.Underliers {
		delta := math.Pow(float64(val)-h.Avg, 2)
		ratio := 1 / float64(h.Count)
		sumVariance += (float64(delta) * ratio)
	}

	return math.Sqrt(sumVariance)
}

func (h *BasicHist) GetSparseBuckets() map[int64]int64 {
	ret := make(map[int64]int64)

	for k, v := range h.Values {
		if v > 0 {
			ret[int64(k)*int64(h.BucketSize)+h.Min] = v
		}
	}

	for _, v := range h.Outliers {
		ret[int64(v)]++
	}

	for _, v := range h.Underliers {
		ret[int64(v)]++
	}

	return ret
}

func (h *BasicHist) GetStrBuckets() map[string]int64 {
	ret := make(map[string]int64)

	for k, v := range h.Values {
		ret[strconv.FormatInt(int64(k)*int64(h.BucketSize)+h.Min, 10)] = v
	}

	for _, v := range h.Outliers {
		ret[strconv.FormatInt(int64(v), 10)]++
	}

	for _, v := range h.Underliers {
		ret[strconv.FormatInt(int64(v), 10)]++
	}

	return ret
}

func (h *BasicHist) Combine(oh Histogram) {
	nextHist := oh.(*HistCompat)

	for k, v := range nextHist.Values {
		h.Values[k] += v
	}

	total := h.Count + nextHist.Count
	h.Avg = (h.Avg * (float64(h.Count) / float64(total))) + (nextHist.Avg * (float64(nextHist.Count) / float64(total)))

	if h.Min > nextHist.Min() {
		h.Min = nextHist.Min()
	}

	if h.Max < nextHist.Max() {
		h.Max = nextHist.Max()
	}

	h.Samples = h.Samples + nextHist.Samples
	h.Count = total
}

func (h *BasicHist) Print() {
	vals := make(map[int64]int64)

	for valIndex, count := range h.Values {
		if count > 0 {
			val := int64(valIndex)*int64(h.BucketSize) + h.Min
			vals[val] = count
		}
	}

	log.Println("HIST COUNTS ARE", vals)
}

// }}} BASIC HIST
