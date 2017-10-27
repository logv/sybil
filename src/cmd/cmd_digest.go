package cmd

import (
	"flag"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/storage/metadata_io"
	. "github.com/logv/sybil/src/storage/row_store"
)

func RunDigestCmdLine() {
	flag.Parse()

	if *FLAGS.TABLE == "" {
		flag.PrintDefaults()
		return
	}

	if *FLAGS.PROFILE {
		profile := RUN_PROFILER()
		defer profile.Start().Stop()
	}

	OPTS.DELETE_BLOCKS_AFTER_QUERY = false

	t := GetTable(*FLAGS.TABLE)
	if LoadTableInfo(t) == false {
		Warn("Couldn't read table info, exiting early")
		return
	}
	DigestRecords(t)
}
