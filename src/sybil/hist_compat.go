package sybil

// {{{ HIST COMPAT WRAPPER FOR BASIC HIST

type HistCompat struct {
	*BasicHist
}

// Min returns the minimum.
func (hc *HistCompat) Min() int64 {

	return hc.BasicHist.Min
}

// Max returns the maximum.
func (hc *HistCompat) Max() int64 {
	return hc.BasicHist.Max
}

func (hc *HistCompat) NewHist(params HistogramParameters) Histogram {
	return hc.table.NewHist(params, &hc.Info, hc.Weighted)
}

// Mean returns the arithmetic mean.
func (hc *HistCompat) Mean() float64 {
	return hc.Avg
}

// GetMeanVariance returns the mean variance.
func (hc *HistCompat) GetMeanVariance() float64 {
	return hc.GetVariance() / float64(hc.Count)
}

// TotalCount returns the total count.
func (hc *HistCompat) TotalCount() int64 {
	return hc.Count
}

// StdDev returns the standard deviation.
func (hc *HistCompat) StdDev() float64 {
	return hc.GetStdDev()
}

// GetIntBuckets returns the integer buckets.
func (hc *HistCompat) GetIntBuckets() map[int64]int64 {
	return hc.GetSparseBuckets()
}

// Range return the range.
func (hc *HistCompat) Range() (int64, int64) {
	return hc.Info.Min, hc.Info.Max
}

// IsWeighted returns if the histogram is weighted.
func (hc *HistCompat) IsWeighted() bool {
	return hc.Weighted
}

// }}}

// {{{ HIST COMPAT WRAPPER FOR MULTI HIST

type MultiHistCompat struct {
	*MultiHist

	Histogram *MultiHist
}

// Min returns the minimum.
func (hc *MultiHistCompat) Min() int64 {

	return hc.Histogram.Min
}

// Max returns the maximum.
func (hc *MultiHistCompat) Max() int64 {
	return hc.Histogram.Max
}

func (hc *MultiHistCompat) NewHist(params HistogramParameters) Histogram {
	return newMultiHist(params, hc.table, hc.Info, hc.Weighted)
}

// Mean returns the arithmetic mean.
func (hc *MultiHistCompat) Mean() float64 {
	return hc.Avg
}

// GetMeanVariance returns the mean variance.
func (hc *MultiHistCompat) GetMeanVariance() float64 {
	return hc.GetVariance() / float64(hc.Count)
}

// TotalCount returns the total count.
func (hc *MultiHistCompat) TotalCount() int64 {
	return hc.Count
}

// StdDev returns the standard deviation.
func (hc *MultiHistCompat) StdDev() float64 {
	return hc.GetStdDev()
}

// GetIntBuckets returns the integer buckets.
func (hc *MultiHistCompat) GetIntBuckets() map[int64]int64 {
	return hc.GetSparseBuckets()
}

// Range return the range.
func (hc *MultiHistCompat) Range() (int64, int64) {
	return hc.Info.Min, hc.Info.Max
}

// IsWeighted returns if the histogram is weighted.
func (hc *MultiHistCompat) IsWeighted() bool {
	return hc.Weighted
}

// }}}
