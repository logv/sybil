package cmd

import (
	"flag"

	"github.com/logv/sybil/src/sybil"
)

func RunVersionCmdLine() {
	flags := sybil.DefaultFlags()
	flags.JSON = flag.Bool("json", false, "Print results in JSON format")
	flag.Parse()

	sybil.PrintVersionInfo(flags)

}
