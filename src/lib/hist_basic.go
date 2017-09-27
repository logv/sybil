package sybil

import "log"
import "math"
import "sort"
import "strconv"

// {{{ BASIC HIST

type BasicHistCachedInfo struct {
	NumBuckets     int
	BucketSize     int
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

type BasicHist struct {
	BasicHistCachedInfo

	table *Table
}

func (h *BasicHist) SetupBuckets(buckets int, min, max int64) {
	// set up initial variables for max and min to be extrema in other
	// direction
	h.Avg = 0
	h.Count = 0
	h.Min = min
	h.Max = max

	if h.PercentileMode {

		h.Outliers = make([]int64, 0)
		h.Underliers = make([]int64, 0)

		size := int64(max - min)
		h.NumBuckets = buckets
		h.BucketSize = int(size / int64(buckets))

		if FLAGS.HIST_BUCKET != nil && *FLAGS.HIST_BUCKET > 0 {
			h.BucketSize = *FLAGS.HIST_BUCKET
		}

		if h.BucketSize == 0 {
			if size < 100 {
				h.BucketSize = 1
				h.NumBuckets = int(size)
			} else {
				h.BucketSize = int(size / int64(100))
				h.NumBuckets = int(size / int64(h.BucketSize))
			}
		}

		h.NumBuckets += 1

		h.Values = make([]int64, h.NumBuckets+1)
		h.Averages = make([]float64, h.NumBuckets+1)
	}
}

func (t *Table) NewHist(info *IntInfo) *HistCompat {

	basic_hist := BasicHist{}
	compat_hist := HistCompat{&basic_hist}
	compat_hist.table = t
	compat_hist.Info = *info

	if FLAGS.OP != nil && *FLAGS.OP == "hist" {
		compat_hist.TrackPercentiles()
	}

	return &compat_hist

}

func (h *BasicHist) TrackPercentiles() {
	h.PercentileMode = true

	h.SetupBuckets(NUM_BUCKETS, h.Info.Min, h.Info.Max)
}

func (h *BasicHist) addValue(value int64) {
	h.addWeightedValue(value, 1)
}

func (h *BasicHist) Sum() int64 {
	return int64(h.Avg * float64(h.Count))
}

func (h *BasicHist) addWeightedValue(value int64, weight int64) {
	// TODO: use more appropriate discard method for .Min to express an order of
	// magnitude
	if value > h.Info.Max*10 || value < h.Info.Min {
		if DEBUG_OUTLIERS {
			log.Println("IGNORING OUTLIER VALUE", value, "MIN IS", h.Info.Min, "MAX IS", h.Info.Max)
		}
		return
	}

	if OPTS.WEIGHT_COL || weight > 1 {
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

	bucket_value := (value - h.Min) / int64(h.BucketSize)

	if bucket_value >= int64(len(h.Values)) {
		h.Outliers = append(h.Outliers, value)
		bucket_value = int64(len(h.Values) - 1)
	}

	if bucket_value < 0 {
		h.Underliers = append(h.Underliers, value)
		bucket_value = 0
	}

	partial := h.Averages[bucket_value]

	// update counts
	h.Values[bucket_value] += weight

	// update bucket averages
	h.Averages[bucket_value] = partial + ((float64(value) - partial) / float64(h.Values[bucket_value]) * float64(weight))
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
	prev_p := int64(0)
	for _, k := range keys {
		key_count := h.Values[k]
		count = count + key_count
		p := (100 * count) / h.Count
		for ip := prev_p; ip <= p; ip++ {
			percentiles[ip] = (int64(k) * int64(h.BucketSize)) + h.Min

		}
		percentiles[p] = int64(k)
		prev_p = p
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

	sum_variance := float64(0)
	for bucket, count := range h.Values {
		val := int64(bucket)*int64(h.BucketSize) + h.Min
		delta := float64(val) - h.Avg

		ratio := float64(count) / float64(h.Count)

		// unbiased variance. probably unstable
		sum_variance += (float64(delta*delta) * ratio)
	}

	for _, val := range h.Outliers {
		delta := math.Pow(float64(val)-h.Avg, 2)
		ratio := 1 / float64(h.Count)
		sum_variance += (float64(delta) * ratio)
	}

	for _, val := range h.Underliers {
		delta := math.Pow(float64(val)-h.Avg, 2)
		ratio := 1 / float64(h.Count)
		sum_variance += (float64(delta) * ratio)
	}

	return math.Sqrt(sum_variance)
}

func (h *BasicHist) GetSparseBuckets() map[int64]int64 {
	ret := make(map[int64]int64, 0)

	for k, v := range h.Values {
		if v > 0 {
			ret[int64(k)*int64(h.BucketSize)+h.Min] = v
		}
	}

	for _, v := range h.Outliers {
		ret[int64(v)] += 1
	}

	for _, v := range h.Underliers {
		ret[int64(v)] += 1
	}

	return ret
}

func (h *BasicHist) GetBuckets() map[string]int64 {
	ret := make(map[string]int64, 0)

	for k, v := range h.Values {
		ret[strconv.FormatInt(int64(k)*int64(h.BucketSize)+h.Min, 10)] = v
	}

	for _, v := range h.Outliers {
		ret[strconv.FormatInt(int64(v), 10)] += 1
	}

	for _, v := range h.Underliers {
		ret[strconv.FormatInt(int64(v), 10)] += 1
	}

	return ret
}

func (h *BasicHist) Combine(oh interface{}) {
	next_hist := oh.(*HistCompat)

	for k, v := range next_hist.Values {
		h.Values[k] += v
	}

	total := h.Count + next_hist.Count
	h.Avg = (h.Avg * (float64(h.Count) / float64(total))) + (next_hist.Avg * (float64(next_hist.Count) / float64(total)))

	if h.Min > next_hist.Min() {
		h.Min = next_hist.Min()
	}

	if h.Max < next_hist.Max() {
		h.Max = next_hist.Max()
	}

	h.Samples = h.Samples + next_hist.Samples
	h.Count = total
}

func (h *BasicHist) Print() {
	vals := make(map[int64]int64)

	for val_index, count := range h.Values {
		if count > 0 {
			val := int64(val_index)*int64(h.BucketSize) + h.Min
			vals[val] = count
		}
	}

	log.Println("HIST COUNTS ARE", vals)
}

// }}} BASIC HIST
