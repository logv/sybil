//+build !tdigest

package sybil

var ENABLE_TDIGEST = false

func (t *Table) NewTDigestHist(info *IntInfo) Histogram {
	return nil

}
