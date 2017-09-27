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

	Subhists []*HistCompat
	Info     *IntInfo
	table    *Table
}

var HIST_FACTOR_POW = uint(1)

func (t *Table) NewMultiHist(info *IntInfo) *MultiHistCompat {

	h := &MultiHist{}
	h.table = t
	h.Info = info

	h.Avg = 0
	h.Count = 0
	h.Min = info.Min
	h.Max = info.Max
	if FLAGS.OP != nil && *FLAGS.OP == "hist" {
		h.TrackPercentiles()
	}

	compat := MultiHistCompat{h, h}
	return &compat
}

func (h *MultiHist) addValue(value int64) {
	h.addWeightedValue(value, 1)
}

func (h *MultiHist) Sum() int64 {
	return int64(h.Avg * float64(h.Count))
}

func (h *MultiHist) addWeightedValue(value int64, weight int64) {
	// TODO: use more appropriate discard method for .Min to express an order of
	// magnitude
	if h.Info != nil {
		if value > h.Info.Max*10 || value < h.Info.Min {
			if DEBUG_OUTLIERS {
				Debug("IGNORING OUTLIER VALUE", value, "MIN IS", h.Info.Min, "MAX IS", h.Info.Max)
			}
			return
		}
	}

	if OPTS.WEIGHT_COL && weight > 1 {
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
			sh.addWeightedValue(value, weight)
			break
		}
	}

}

func (h *MultiHist) GetPercentiles() []int64 {
	if h.Count == 0 {
		return make([]int64, 0)
	}

	all_buckets := h.GetSparseBuckets()

	buckets := make([]int, 0)
	total := int64(0)
	for bucket, count := range all_buckets {
		if all_buckets[bucket] > 0 {
			buckets = append(buckets, int(bucket))
			total += count
		}
	}

	sort.Ints(buckets)

	prev_p := int64(0)
	count := int64(0)
	percentiles := make([]int64, 101)
	for _, k := range buckets {
		key_count := all_buckets[int64(k)]
		count = count + key_count
		p := (100 * count) / total
		for ip := prev_p; ip <= p; ip++ {
			if ip <= 100 {
				percentiles[ip] = int64(k)
			}

		}

		if p <= 100 {
			percentiles[p] = int64(k)
		} else if DEBUG_OUTLIERS {
			Print("SETTING P", p, k)
		}
		prev_p = p
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
	all_buckets := h.GetSparseBuckets()

	sum_variance := float64(0)
	for val, count := range all_buckets {
		delta := float64(val) - h.Avg

		ratio := float64(count) / float64(h.Count)

		// unbiased variance. probably unstable
		sum_variance += (float64(delta*delta) * ratio)
	}

	return math.Sqrt(sum_variance)
}

func (h *MultiHist) GetNonZeroBuckets() map[string]int64 {
	non_zero_buckets := make(map[string]int64)
	buckets := h.GetBuckets()
	for k, v := range buckets {
		if v > 0 {
			non_zero_buckets[k] = v
		}
	}

	return non_zero_buckets

}

func (h *MultiHist) GetBuckets() map[string]int64 {
	all_buckets := make(map[string]int64, 0)
	for _, subhist := range h.Subhists {
		for key, count := range subhist.GetBuckets() {
			all_buckets[key] = count
		}
	}

	return all_buckets
}

func (h *MultiHist) GetSparseBuckets() map[int64]int64 {
	all_buckets := make(map[int64]int64, 0)
	for _, subhist := range h.Subhists {
		for key, count := range subhist.GetSparseBuckets() {
			_, ok := all_buckets[key]

			if !ok {
				all_buckets[key] = count
			} else {
				all_buckets[key] += count
			}
		}
	}

	return all_buckets

}

func (h *MultiHist) Combine(oh interface{}) {
	next_hist := oh.(*MultiHistCompat)
	for i, subhist := range h.Subhists {
		subhist.Combine(next_hist.Subhists[i])
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

func (h *MultiHist) TrackPercentiles() {
	h.PercentileMode = true
	BucketSize := (h.Max - h.Min)

	// We create 1:1 buckets for the smallest bucket, then increase
	// logarithmically
	num_hists := 0
	for t := BucketSize; t > int64(NUM_BUCKETS); t >>= HIST_FACTOR_POW {
		num_hists += 1
	}

	h.Subhists = make([]*HistCompat, num_hists+1)

	right_edge := h.Max

	for i := 0; i < num_hists; i++ {
		BucketSize >>= HIST_FACTOR_POW
		info := IntInfo{}
		info.Min = right_edge - BucketSize
		info.Max = right_edge

		right_edge = info.Min
		h.Subhists[i] = h.table.NewHist(&info)
		h.Subhists[i].TrackPercentiles()
	}

	// Add the smallest hist to the end from h.Min -> the last bucket
	info := IntInfo{}
	info.Min = h.Min
	info.Max = right_edge

	h.Subhists[num_hists] = h.table.NewHist(&info)
	h.Subhists[num_hists].TrackPercentiles()

}

func (h *MultiHist) Print() {
	Debug("HIST COUNTS ARE", 0)
}
