//+build !hdrhist

package sybil

var EnableHdr = false

func newHDRHist(table *Table, info *IntInfo) Histogram {
	return table.NewHist(info)
}
