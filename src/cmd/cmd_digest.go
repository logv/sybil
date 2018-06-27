package cmd

import (
	"flag"
	"fmt"
	"os"

	"github.com/logv/sybil/src/internal/internalpb"
	"github.com/logv/sybil/src/sybil"
	"github.com/pkg/errors"
)

func RunDigestCmdLine() {
	flag.Parse()

	if err := runDigestCmdLine(&sybil.FLAGS); err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrap(err, "digest"))
		os.Exit(1)
	}
}

func runDigestCmdLine(flags *internalpb.FlagDefs) error {
	if flags.TABLE == "" {
		flag.PrintDefaults()
		return sybil.ErrMissingTable
	}

	if flags.PROFILE {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}
	t := sybil.GetTable(flags.TABLE)
	if err := t.LoadTableInfo(); err != nil {
		return err
	}
	return t.DigestRecords()
}
