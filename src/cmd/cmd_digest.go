package cmd

import (
	"flag"

	"github.com/logv/sybil/src/sybil"
)

func RunDigestCmdLine() {
	flag.Parse()

	flags := sybil.DefaultFlags()
	if *flags.TABLE == "" {
		flag.PrintDefaults()
		return
	}

	if *flags.PROFILE {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	t := sybil.GetTable(*flags.TABLE)
	if !t.LoadTableInfo(flags) {
		sybil.Warn("Couldn't read table info, exiting early")
		return
	}
	t.DigestRecords(flags)
}
