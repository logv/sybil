package sybil

import "log"
import "math"
import "sort"
import "strconv"

// {{{ BASIC HIST
type BasicHist struct {
	Max     int64
	Min     int64
	Samples int
	Count   int64
	Avg     float64

	num_buckets       int
	bucket_size       int
	values            []int64
	avgs              []float64
	track_percentiles bool

	outliers   []int64
	underliers []int64

	table *Table
	info  *IntInfo
}

func (h *BasicHist) SetupBuckets(buckets int, min, max int64) {
	// set up initial variables for max and min to be extrema in other
	// direction
	h.Avg = 0
	h.Count = 0
	h.Min = min
	h.Max = max

	if h.track_percentiles {

		h.outliers = make([]int64, 0)
		h.underliers = make([]int64, 0)

		size := int64(max - min)
		h.num_buckets = buckets
		h.bucket_size = int(size / int64(buckets))

		if FLAGS.HIST_BUCKET != nil && *FLAGS.HIST_BUCKET > 0 {
			h.bucket_size = *FLAGS.HIST_BUCKET
		}

		if h.bucket_size == 0 {
			if size < 100 {
				h.bucket_size = 1
				h.num_buckets = int(size)
			} else {
				h.bucket_size = int(size / int64(100))
				h.num_buckets = int(size / int64(h.bucket_size))
			}
		}

		h.num_buckets += 1

		h.values = make([]int64, h.num_buckets+1)
		h.avgs = make([]float64, h.num_buckets+1)
	}
}

func (t *Table) NewHist(info *IntInfo) *HistCompat {

	basic_hist := BasicHist{}
	compat_hist := HistCompat{&basic_hist, &basic_hist}
	compat_hist.table = t
	compat_hist.info = info
	compat_hist.Histogram = &basic_hist

	if FLAGS.OP != nil && *FLAGS.OP == "hist" {
		compat_hist.TrackPercentiles()
	}

	return &compat_hist

}

func (h *BasicHist) TrackPercentiles() {
	h.track_percentiles = true

	h.SetupBuckets(NUM_BUCKETS, h.info.Min, h.info.Max)
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
	if value > h.info.Max*10 || value < h.info.Min {
		if DEBUG_OUTLIERS {
			log.Println("IGNORING OUTLIER VALUE", value, "MIN IS", h.info.Min, "MAX IS", h.info.Max)
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

	if !h.track_percentiles {
		return
	}

	bucket_value := (value - h.Min) / int64(h.bucket_size)

	if bucket_value >= int64(len(h.values)) {
		h.outliers = append(h.outliers, value)
		bucket_value = int64(len(h.values) - 1)
	}

	if bucket_value < 0 {
		h.underliers = append(h.underliers, value)
		bucket_value = 0
	}

	partial := h.avgs[bucket_value]

	// update counts
	h.values[bucket_value] += weight

	// update bucket averages
	h.avgs[bucket_value] = partial + ((float64(value) - partial) / float64(h.values[bucket_value]) * float64(weight))
}

func (h *BasicHist) GetPercentiles() []int64 {
	if h.Count == 0 {
		return make([]int64, 0)
	}

	percentiles := make([]int64, 101)
	keys := make([]int, 0)

	// unpack the bucket values!
	for k := range h.values {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	percentiles[0] = h.Min
	count := int64(0)
	prev_p := int64(0)
	for _, k := range keys {
		key_count := h.values[k]
		count = count + key_count
		p := (100 * count) / h.Count
		for ip := prev_p; ip <= p; ip++ {
			percentiles[ip] = (int64(k) * int64(h.bucket_size)) + h.Min

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
	for bucket, count := range h.values {
		val := int64(bucket)*int64(h.bucket_size) + h.Min
		delta := float64(val) - h.Avg

		ratio := float64(count) / float64(h.Count)

		// unbiased variance. probably unstable
		sum_variance += (float64(delta*delta) * ratio)
	}

	for _, val := range h.outliers {
		delta := math.Pow(float64(val)-h.Avg, 2)
		ratio := 1 / float64(h.Count)
		sum_variance += (float64(delta) * ratio)
	}

	for _, val := range h.underliers {
		delta := math.Pow(float64(val)-h.Avg, 2)
		ratio := 1 / float64(h.Count)
		sum_variance += (float64(delta) * ratio)
	}

	return math.Sqrt(sum_variance)
}

func (h *BasicHist) GetSparseBuckets() map[int64]int64 {
	ret := make(map[int64]int64, 0)

	for k, v := range h.values {
		if v > 0 {
			ret[int64(k)*int64(h.bucket_size)+h.Min] = v
		}
	}

	for _, v := range h.outliers {
		ret[int64(v)] += 1
	}

	for _, v := range h.underliers {
		ret[int64(v)] += 1
	}

	return ret
}

func (h *BasicHist) GetBuckets() map[string]int64 {
	ret := make(map[string]int64, 0)

	for k, v := range h.values {
		ret[strconv.FormatInt(int64(k)*int64(h.bucket_size)+h.Min, 10)] = v
	}

	for _, v := range h.outliers {
		ret[strconv.FormatInt(int64(v), 10)] += 1
	}

	for _, v := range h.underliers {
		ret[strconv.FormatInt(int64(v), 10)] += 1
	}

	return ret
}

func (h *BasicHist) Combine(oh interface{}) {
	next_hist := oh.(*HistCompat)

	for k, v := range next_hist.values {
		h.values[k] += v
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

	for val_index, count := range h.values {
		if count > 0 {
			val := int64(val_index)*int64(h.bucket_size) + h.Min
			vals[val] = count
		}
	}

	log.Println("HIST COUNTS ARE", vals)
}

// }}} BASIC HIST
