package cmd

import (
	"flag"

	sybil "github.com/logv/sybil/src/lib"
	"github.com/logv/sybil/src/lib/common"
)

func RunDigestCmdLine() {
	flag.Parse()

	if *common.FLAGS.TABLE == "" {
		flag.PrintDefaults()
		return
	}

	if *common.FLAGS.PROFILE {
		profile := common.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	sybil.DELETE_BLOCKS_AFTER_QUERY = false

	t := sybil.GetTable(*common.FLAGS.TABLE)
	if t.LoadTableInfo() == false {
		common.Warn("Couldn't read table info, exiting early")
		return
	}
	t.DigestRecords()
}
