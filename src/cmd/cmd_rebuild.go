package cmd

import (
	"flag"
	"fmt"
	"os"

	"github.com/logv/sybil/src/internal/internalpb"
	"github.com/logv/sybil/src/sybil"
	"github.com/pkg/errors"
)

func RunRebuildCmdLine() {
	REPLACE_INFO := flag.Bool("replace", false, "Replace broken info.db if it exists")
	FORCE_UPDATE := flag.Bool("force", false, "Force re-calculation of info.db, even if it exists")
	flag.Parse()
	if err := runRebuildCmdLine(&sybil.FLAGS, *REPLACE_INFO, *FORCE_UPDATE); err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrap(err, "digest"))
		os.Exit(1)
	}
}

func runRebuildCmdLine(flags *internalpb.FlagDefs, replaceInfo bool, forceUpdate bool) error {
	if flags.TABLE == "" {
		flag.PrintDefaults()
		return sybil.ErrMissingTable
	}

	if flags.PROFILE {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	t := sybil.GetTable(flags.TABLE)

	loadErr := t.LoadTableInfo()
	if loadErr != nil {
		return loadErr
	}
	loaded := loadErr == nil && !forceUpdate
	if loaded {
		sybil.Print("TABLE INFO ALREADY EXISTS, NOTHING TO REBUILD!")
		return nil
	}

	t.DeduceTableInfoFromBlocks()

	// TODO: prompt to see if this table info looks good and then write it to
	// original info.db
	if replaceInfo {
		sybil.Print("REPLACING info.db WITH DATA COMPUTED ABOVE")
		lock := sybil.Lock{Table: t, Name: "info"}
		if err := lock.ForceDeleteFile(); err != nil {
			return err
		}
		return t.SaveTableInfo("info")
	} else {
		sybil.Print("SAVING TO temp_info.db")
		return t.SaveTableInfo("temp_info")
	}
}
