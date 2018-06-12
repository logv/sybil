package sybil_cmd

import sybil "github.com/logv/sybil/src/lib"

import "flag"

// our aggregate command will take multiple output files on the command in,
// verify all their query specs match the same md5 result and then combine them

func RunAggregateCmdLine() {
	flag.Parse()
	dirs := flag.Args()

	sybil.FLAGS.DEBUG = true
	sybil.Debug("AGGREGATING")

	sybil.DecodeFlags()
	sybil.FLAGS.PRINT = true
	sybil.FLAGS.ENCODE_RESULTS = false
	sybil.Debug("AGGREGATING DIRS", dirs)

	vt := sybil.VTable{}
	vt.StitchResults(dirs)

}
