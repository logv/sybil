package sybilCmd

import "flag"

import sybil "github.com/logv/sybil/src/lib"

func RunDigestCmdLine() {
	flag.Parse()

	if *sybil.FLAGS.Table == "" {
		flag.PrintDefaults()
		return
	}

	if *sybil.FLAGS.Profile {
		profile := sybil.RunProfiler()
		defer profile.Start().Stop()
	}

	sybil.DeleteBlocksAfterQuery = false

	t := sybil.GetTable(*sybil.FLAGS.Table)
	if t.LoadTableInfo() == false {
		sybil.Warn("Couldn't read table info, exiting early")
		return
	}
	t.DigestRecords()
}
