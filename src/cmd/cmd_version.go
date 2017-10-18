package cmd

import (
	"flag"

	sybil "github.com/logv/sybil/src/lib"
	"github.com/logv/sybil/src/lib/config"
)

func RunVersionCmdLine() {
	config.FLAGS.JSON = flag.Bool("json", false, "Print results in JSON format")
	flag.Parse()
	sybil.PrintVersionInfo()
}
