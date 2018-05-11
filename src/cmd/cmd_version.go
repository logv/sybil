package cmd

import sybil "github.com/logv/sybil/src/lib"
import "flag"

func RunVersionCmdLine() {
	sybil.FLAGS.JSON = flag.Bool("json", false, "Print results in JSON format")
	flag.Parse()

	sybil.PrintVersionInfo()

}
