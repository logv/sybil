package cmd

import (
	"flag"
	"fmt"
	"os"

	"github.com/logv/sybil/src/sybil"
)

// our aggregate command will take multiple output files on the command in,
// verify all their query specs match the same md5 result and then combine them

func RunAggregateCmdLine() {
	addPrintFlags()
	flag.Parse()
	dirs := flag.Args()

	sybil.FLAGS.DEBUG = true
	sybil.Debug("AGGREGATING")

	if err := sybil.DecodeFlags(); err != nil {
		fmt.Fprintln(os.Stderr, "aggregate: failed to decode flags:", err)
	}
	sybil.FLAGS.PRINT = true
	sybil.FLAGS.ENCODE_RESULTS = false
	sybil.Debug("AGGREGATING DIRS", dirs)

	printSpec := &sybil.PrintSpec{
		ListTables: sybil.FLAGS.LIST_TABLES,
		PrintInfo:  sybil.FLAGS.PRINT_INFO,
		Samples:    sybil.FLAGS.SAMPLES,

		Op:    sybil.Op(sybil.FLAGS.OP),
		Limit: sybil.FLAGS.LIMIT,
		JSON:  sybil.FLAGS.JSON,
	}
	vt := sybil.VTable{}
	vt.StitchResults(printSpec, dirs)

}
