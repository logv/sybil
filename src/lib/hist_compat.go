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

func (h *HistCompat) Mean() float64 {
	return h.Avg
}

func (h *HistCompat) GetMeanVariance() float64 {
	return h.GetVariance() / float64(h.Count)
}

func (h *HistCompat) TotalCount() int64 {
	return h.Count
}

func (h *HistCompat) StdDev() float64 {
	return h.GetStdDev()
}

// compat layer with hdr hist
func (h *HistCompat) RecordValues(value int64, n int64) error {
	h.addWeightedValue(value, n)

	return nil
}

func (h *HistCompat) Distribution() map[string]int64 {
	return h.GetBuckets()
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
	return hc.table.NewMultiHist(hc.Info)
}

func (h *MultiHistCompat) Mean() float64 {
	return h.Avg
}

func (h *MultiHistCompat) GetMeanVariance() float64 {
	return h.GetVariance() / float64(h.Count)
}

func (h *MultiHistCompat) TotalCount() int64 {
	return h.Count
}

func (h *MultiHistCompat) StdDev() float64 {
	return h.GetStdDev()
}

// compat layer with hdr hist
func (h *MultiHistCompat) RecordValues(value int64, n int64) error {
	h.addWeightedValue(value, n)

	return nil
}

func (h *MultiHistCompat) Distribution() map[string]int64 {
	return h.GetBuckets()
}

// }}}
