package sybil

import "sort"
import "math"

type MultiHist struct {
	Max     int64
	Min     int64
	Samples int
	Count   int64
	Avg     float64

	PercentileMode bool
	Weighted       bool `json:",omitempty"`

	HistogramParameters HistogramParameters

	Subhists []*HistCompat
	Info     IntInfo
}

var HIST_FACTOR_POW = uint(1)

func newMultiHist(params HistogramParameters, info IntInfo) *MultiHistCompat {

	h := &MultiHist{}
	h.HistogramParameters = params
	h.Info = info

	h.Avg = 0
	h.Count = 0
	h.Min = info.Min
	h.Max = info.Max
	if params.Type != HistogramTypeNone {
		h.TrackPercentiles()
	}

	compat := MultiHistCompat{h, h}
	return &compat
}

func (h *MultiHist) AddValue(value int64) {
	h.AddWeightedValue(value, 1)
}

func (h *MultiHist) Sum() int64 {
	return int64(h.Avg * float64(h.Count))
}

func (h *MultiHist) AddWeightedValue(value int64, weight int64) {
	// TODO: use more appropriate discard method for .Min to express an order of
	// magnitude
	if value > h.Info.Max*10 || value < h.Info.Min {
		if DEBUG_OUTLIERS {
			Debug("IGNORING OUTLIER VALUE", value, "MIN IS", h.Info.Min, "MAX IS", h.Info.Max)
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

	for _, sh := range h.Subhists {
		if value >= sh.Info.Min && value <= sh.Info.Max {
			sh.AddWeightedValue(value, weight)
			break
		}
	}

}

func (h *MultiHist) GetPercentiles() []int64 {
	if h.Count == 0 {
		return make([]int64, 0)
	}

	allBuckets := h.GetSparseBuckets()

	buckets := make([]int, 0)
	total := int64(0)
	for bucket, count := range allBuckets {
		if allBuckets[bucket] > 0 {
			buckets = append(buckets, int(bucket))
			total += count
		}
	}

	sort.Ints(buckets)

	prevP := int64(0)
	count := int64(0)
	percentiles := make([]int64, 101)
	for _, k := range buckets {
		keyCount := allBuckets[int64(k)]
		count = count + keyCount
		p := (100 * count) / total
		for ip := prevP; ip <= p; ip++ {
			if ip <= 100 {
				percentiles[ip] = int64(k)
			}

		}

		if p <= 100 {
			percentiles[p] = int64(k)
		} else if DEBUG_OUTLIERS {
			Print("SETTING P", p, k)
		}
		prevP = p
	}

	return percentiles[:100]
}

func (h *MultiHist) GetMeanVariance() float64 {
	return h.GetVariance() / float64(h.Count)
}

func (h *MultiHist) GetVariance() float64 {
	std := h.GetStdDev()
	return std * std
}

// VARIANCE is defined as the squared error from the mean
// STD DEV is defined as sqrt(VARIANCE)
func (h *MultiHist) GetStdDev() float64 {
	allBuckets := h.GetSparseBuckets()

	sumVariance := float64(0)
	for val, count := range allBuckets {
		delta := float64(val) - h.Avg

		ratio := float64(count) / float64(h.Count)

		// unbiased variance. probably unstable
		sumVariance += (float64(delta*delta) * ratio)
	}

	return math.Sqrt(sumVariance)
}

func (h *MultiHist) GetNonZeroBuckets() map[string]int64 {
	nonZeroBuckets := make(map[string]int64)
	buckets := h.GetStrBuckets()
	for k, v := range buckets {
		if v > 0 {
			nonZeroBuckets[k] = v
		}
	}

	return nonZeroBuckets

}

func (h *MultiHist) GetStrBuckets() map[string]int64 {
	allBuckets := make(map[string]int64)
	for _, subhist := range h.Subhists {
		for key, count := range subhist.GetStrBuckets() {
			allBuckets[key] = count
		}
	}

	return allBuckets
}

func (h *MultiHist) GetSparseBuckets() map[int64]int64 {
	allBuckets := make(map[int64]int64)
	for _, subhist := range h.Subhists {
		for key, count := range subhist.GetSparseBuckets() {
			_, ok := allBuckets[key]

			if !ok {
				allBuckets[key] = count
			} else {
				allBuckets[key] += count
			}
		}
	}

	return allBuckets

}

func (h *MultiHist) Combine(oh Histogram) {
	nextHist := oh.(*MultiHistCompat)
	for i, subhist := range h.Subhists {
		subhist.Combine(nextHist.Subhists[i])
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

func (h *MultiHist) TrackPercentiles() {
	h.PercentileMode = true
	BucketSize := (h.Max - h.Min)

	// We create 1:1 buckets for the smallest bucket, then increase
	// logarithmically
	numHists := 0
	for t := BucketSize; t > int64(NUM_BUCKETS); t >>= HIST_FACTOR_POW {
		numHists++
	}

	h.Subhists = make([]*HistCompat, numHists+1)

	rightEdge := h.Max

	for i := 0; i < numHists; i++ {
		BucketSize >>= HIST_FACTOR_POW
		info := IntInfo{}
		info.Min = rightEdge - BucketSize
		info.Max = rightEdge

		rightEdge = info.Min
		h.Subhists[i] = newBasicHist(h.HistogramParameters, info)
	}

	// Add the smallest hist to the end from h.Min -> the last bucket
	info := IntInfo{}
	info.Min = h.Min
	info.Max = rightEdge

	h.Subhists[numHists] = newBasicHist(h.HistogramParameters, info)

}

func (h *MultiHist) Print() {
	Debug("HIST COUNTS ARE", 0)
}
