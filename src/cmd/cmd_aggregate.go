package cmd

import (
	"flag"

	"github.com/logv/sybil/src/sybil"
)

// our aggregate command will take multiple output files on the command in,
// verify all their query specs match the same md5 result and then combine them

func RunAggregateCmdLine() {
	flags := sybil.DefaultFlags()
	flag.Parse()
	dirs := flag.Args()

	var t, f = true, false
	sybil.DEBUG = &t
	sybil.Debug("AGGREGATING")

	sybil.DecodeFlags(flags)
	flags.PRINT = &t
	flags.ENCODE_RESULTS = &f
	sybil.Debug("AGGREGATING DIRS", dirs)

	vt := sybil.VTable{}
	vt.StitchResults(flags, dirs)

}
