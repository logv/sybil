package sybil

// {{{ HIST COMPAT WRAPPER FOR BASIC HIST

type HistCompat struct {
	*BasicHist
}

func (hc *HistCompat) Min() int64 {

	return hc.BasicHist.Min
}

func (hc *HistCompat) Max() int64 {
	return hc.BasicHist.Max
}

func (hc *HistCompat) NewHist() Histogram {
	return hc.table.NewHist(&hc.Info)
}

func (hc *HistCompat) Mean() float64 {
	return hc.Avg
}

func (hc *HistCompat) GetMeanVariance() float64 {
	return hc.GetVariance() / float64(hc.Count)
}

func (hc *HistCompat) TotalCount() int64 {
	return hc.Count
}

func (hc *HistCompat) StdDev() float64 {
	return hc.GetStdDev()
}

func (hc *HistCompat) GetIntBuckets() map[int64]int64 {
	return hc.GetSparseBuckets()
}

func (hc *HistCompat) Range() (int64, int64) {
	return hc.Info.Min, hc.Info.Max
}

// }}}

// {{{ HIST COMPAT WRAPPER FOR MULTI HIST

type MultiHistCompat struct {
	*MultiHist

	Histogram *MultiHist
}

func (hc *MultiHistCompat) Min() int64 {

	return hc.Histogram.Min
}

func (hc *MultiHistCompat) Max() int64 {
	return hc.Histogram.Max
}

func (hc *MultiHistCompat) NewHist() Histogram {
	return newMultiHist(hc.table, hc.Info)
}

func (hc *MultiHistCompat) Mean() float64 {
	return hc.Avg
}

func (hc *MultiHistCompat) GetMeanVariance() float64 {
	return hc.GetVariance() / float64(hc.Count)
}

func (hc *MultiHistCompat) TotalCount() int64 {
	return hc.Count
}

func (hc *MultiHistCompat) StdDev() float64 {
	return hc.GetStdDev()
}

func (hc *MultiHistCompat) GetIntBuckets() map[int64]int64 {
	return hc.GetSparseBuckets()
}

func (hc *MultiHistCompat) Range() (int64, int64) {
	return hc.Info.Min, hc.Info.Max
}

// }}}
