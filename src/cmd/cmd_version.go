package sybil_cmd

import sybil "github.com/logv/sybil/src/lib"
import "flag"

func RunVersionCmdLine() {
	flag.BoolVar(&sybil.FLAGS.JSON, "json", false, "Print results in JSON format")
	flag.Parse()

	sybil.PrintVersionInfo()

}
