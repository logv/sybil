package sybil

import "sort"

type Hist struct {
	Max     int
	Min     int
	Samples int
	Count   int64
	Avg     float64

	num_buckets       int
	bucket_size       int
	values            []int64
	avgs              []float64
	track_percentiles bool

	outliers   []int
	underliers []int
}

func (t *Table) NewHist(info *IntInfo) *Hist {

	buckets := 200 // resolution?
	h := &Hist{}

	h.num_buckets = buckets

	// set up initial variables for max and min to be extrema in other
	// direction
	h.Max = int(info.Min)
	h.Min = int(info.Max)

	h.Avg = 0
	h.Count = 0

	h.outliers = make([]int, 0)
	h.underliers = make([]int, 0)

	size := info.Max - info.Min
	h.bucket_size = int(size / int64(buckets))
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

	return h
}

func (h *Hist) TrackPercentiles() {
	h.track_percentiles = true
}

func (h *Hist) addValue(value int, weight int64) {
	h.Samples++
	h.Count += weight
	h.Avg = h.Avg + (float64(value)-h.Avg)/float64(h.Count)*float64(weight)

	if value > h.Max {
		h.Max = value
	}

	if value < h.Min {
		h.Min = value
	}

	if !h.track_percentiles {
		return
	}

	bucket_value := (value - h.Min) / h.bucket_size

	if bucket_value >= len(h.avgs) {
		h.outliers = append(h.outliers, value)
		return
	}

	if bucket_value < 0 {
		h.underliers = append(h.underliers, value)
		return
	}

	partial := h.avgs[bucket_value]

	// update counts
	h.values[bucket_value] += weight

	// update bucket averages
	h.avgs[bucket_value] = partial + ((float64(value) - partial) / float64(h.values[bucket_value]) * float64(weight))
}

func (h *Hist) getPercentiles() []int {
	if h.Count == 0 {
		return make([]int, 0)
	}

	percentiles := make([]int, 101)
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
		p := 100 * count / h.Count
		for ip := prev_p; ip < p; ip++ {
			percentiles[ip] = (k * h.bucket_size) + h.Min
		}
		percentiles[p] = k
		prev_p = p
	}

	return percentiles
}

func (h *Hist) Combine(next_hist *Hist) {
	for k, v := range next_hist.values {
		h.values[k] += v
	}

	total := h.Count + next_hist.Count
	h.Avg = (h.Avg * (float64(h.Count) / float64(total))) + (next_hist.Avg * (float64(next_hist.Count) / float64(total)))

	if h.Min > next_hist.Min {
		h.Min = next_hist.Min
	}

	if h.Max < next_hist.Max {
		h.Max = next_hist.Max
	}

	h.Count = total
}
