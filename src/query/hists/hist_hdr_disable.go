//+build !hdrhist

package hists

import . "github.com/logv/sybil/src/lib/structs"

var ENABLE_HDR = false

func newHDRHist(table *Table, info *IntInfo) Histogram {
	return NewHist(table, info)
}
