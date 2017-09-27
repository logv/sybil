//+build hdrhist

package sybil

import "strconv"
import "github.com/codahale/hdrhistogram"

var ENABLE_HDR = true

func newHDRHist(table *Table, info *IntInfo) Histogram {
	return table.NewHDRHist(info)
}

// {{{ HDR HIST
type HDRHist struct {
	*hdrhistogram.Histogram

	table *Table
	info  *IntInfo

	PercentileMode bool
}

func (th *HDRHist) NewHist() Histogram {
	return th.table.NewHDRHist(th.info)
}

func (t *Table) NewHDRHist(info *IntInfo) *HDRHist {
	hdr_hist := hdrhistogram.New(info.Min, info.Max*2, 5)
	outer_hist := HDRHist{hdr_hist, t, info, true}

	return &outer_hist

}

func (th *HDRHist) Combine(oh interface{}) {
	hist := oh.(*HDRHist)
	th.Histogram.Merge(hist.Histogram)
}

func (th *HDRHist) GetVariance() float64 {
	std := th.StdDev()
	return std * std
}

func (th *HDRHist) GetPercentiles() []int64 {

	ret := make([]int64, 100)
	for i := 0; i < 100; i++ {
		ret[i] = th.ValueAtQuantile(float64(i))
	}

	return ret
}

func (th *HDRHist) GetBuckets() map[string]int64 {
	ret := make(map[string]int64)
	for _, v := range th.Distribution() {
		key := strconv.FormatInt(int64(v.From+v.To)/2, 10)
		ret[key] = v.Count
	}

	return ret

}

// }}} HDR HIST
