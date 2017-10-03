package cmd

import (
	"flag"

	"github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/printer"
)

func RunVersionCmdLine() {
	config.FLAGS.JSON = flag.Bool("json", false, "Print results in JSON format")
	flag.Parse()
	PrintVersionInfo()
}
