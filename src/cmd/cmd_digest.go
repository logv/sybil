package cmd

import (
	"flag"

	. "github.com/logv/sybil/src/lib/column_store"
	"github.com/logv/sybil/src/lib/common"
	"github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/ingest"
	. "github.com/logv/sybil/src/lib/locks"
	. "github.com/logv/sybil/src/lib/structs"
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

	DELETE_BLOCKS_AFTER_QUERY = false

	t := GetTable(*config.FLAGS.TABLE)
	if LoadTableInfo(t) == false {
		common.Warn("Couldn't read table info, exiting early")
		return
	}
	DigestRecords(t)
}
