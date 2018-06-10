package cmd

import (
	"flag"

	"github.com/logv/sybil/src/sybil"
)

// our aggregate command will take multiple output files on the command in,
// verify all their query specs match the same md5 result and then combine them

func RunAggregateCmdLine() {
	addPrintFlags()
	flag.Parse()
	dirs := flag.Args()

	var t, f = true, false
	sybil.FLAGS.DEBUG = &t
	sybil.Debug("AGGREGATING")

	sybil.DecodeFlags()
	sybil.FLAGS.PRINT = &t
	sybil.FLAGS.ENCODE_RESULTS = &f
	sybil.Debug("AGGREGATING DIRS", dirs)

	printSpec := &sybil.PrintSpec{
		ListTables: *sybil.FLAGS.LIST_TABLES,
		PrintInfo:  *sybil.FLAGS.PRINT_INFO,
		Samples:    *sybil.FLAGS.SAMPLES,

		Op:    *sybil.FLAGS.OP,
		Limit: *sybil.FLAGS.LIMIT,
		JSON:  *sybil.FLAGS.JSON,
	}
	vt := sybil.VTable{}
	vt.StitchResults(printSpec, dirs)

}
