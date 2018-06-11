package cmd

import (
	"flag"

	"github.com/logv/sybil/src/sybil"
)

func RunVersionCmdLine() {
	flag.BoolVar(&sybil.FLAGS.JSON, "json", false, "Print results in JSON format")
	flag.Parse()

	sybil.PrintVersionInfo()

}
