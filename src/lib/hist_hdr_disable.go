//+build !hdrhist

package sybil

var ENABLE_HDR = false

func newHDRHist(table *Table, info *IntInfo) Histogram {
	return table.NewHist(info)
}
