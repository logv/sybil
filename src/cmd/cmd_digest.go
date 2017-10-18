package cmd

import (
	"flag"

	sybil "github.com/logv/sybil/src/lib"
	"github.com/logv/sybil/src/lib/common"
	"github.com/logv/sybil/src/lib/config"
)

func RunDigestCmdLine() {
	flag.Parse()

	if *config.FLAGS.TABLE == "" {
		flag.PrintDefaults()
		return
	}

	if *config.FLAGS.PROFILE {
		profile := config.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	sybil.DELETE_BLOCKS_AFTER_QUERY = false

	t := sybil.GetTable(*config.FLAGS.TABLE)
	if t.LoadTableInfo() == false {
		common.Warn("Couldn't read table info, exiting early")
		return
	}
	t.DigestRecords()
}
