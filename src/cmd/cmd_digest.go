package cmd

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/logv/sybil/src/sybil"
)

func RunDigestCmdLine() {
	flag.Parse()

	if sybil.FLAGS.TABLE == "" {
		flag.PrintDefaults()
		return
	}

	if sybil.FLAGS.PROFILE {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}
	t := sybil.GetTable(sybil.FLAGS.TABLE)
	if !t.LoadTableInfo() {
		// TODO use LoadTableInfo
		err := errors.New("digest: Couldn't read table info, exiting early")
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	t.DigestRecords()
}
