package cmd

import (
	"flag"

	. "github.com/logv/sybil/src/lib/config"
	printer "github.com/logv/sybil/src/query/printer"
)

func RunVersionCmdLine() {
	FLAGS.JSON = flag.Bool("json", false, "Print results in JSON format")
	flag.Parse()
	printer.PrintVersionInfo()
}
