package cmd

import (
	"flag"
	"fmt"
	"os"

	"github.com/logv/sybil/src/sybil"
	"github.com/pkg/errors"
)

func askConfirmation() bool {

	var response string
	_, err := fmt.Scanln(&response)
	if err != nil {
		sybil.Error(err)
	}

	if response == "Y" {
		return true
	}

	if response == "N" {
		return false
	}

	fmt.Println("Y or N only")
	return askConfirmation()

}

func RunTrimCmdLine() {
	MB_LIMIT := flag.Int("mb", 0, "max table size in MB")
	DELETE_BEFORE := flag.Int("before", 0, "delete blocks with data older than TIMESTAMP")
	DELETE := flag.Bool("delete", false, "delete blocks? be careful! will actually delete your data!")
	REALLY := flag.Bool("really", false, "don't prompt before deletion")

	flag.StringVar(&sybil.FLAGS.TIME_COL, "time-col", "", "which column to treat as a timestamp [REQUIRED]")
	flag.Parse()
	err := runTrimCmdLine(&sybil.FLAGS, *MB_LIMIT, *DELETE_BEFORE, !*REALLY, *DELETE)
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrap(err, "trim"))
		os.Exit(1)
	}
}

func runTrimCmdLine(flags *sybil.FlagDefs, mbLimit int, deleteBefore int, skipPrompt bool, delete bool) error {
	if flags.TABLE == "" || flags.TIME_COL == "" {
		flag.PrintDefaults()
		return sybil.ErrMissingTable
	}

	if flags.PROFILE {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	t := sybil.GetTable(flags.TABLE)
	if !t.LoadTableInfo() {
		// TODO use LoadTableInfo
		return errors.New("Couldn't read table info, exiting early")
	}

	loadSpec := t.NewLoadSpec()
	loadSpec.SkipDeleteBlocksAfterQuery = true
	if err := loadSpec.Int(flags.TIME_COL); err != nil {
		return err
	}

	trimSpec := sybil.TrimSpec{}
	trimSpec.DeleteBefore = int64(deleteBefore)
	trimSpec.MBLimit = int64(mbLimit)

	toTrim := t.TrimTable(&trimSpec)

	sybil.Debug("FOUND", len(toTrim), "CANDIDATE BLOCKS FOR TRIMMING")
	if len(toTrim) > 0 {
		for _, b := range toTrim {
			fmt.Println(b.Name)
		}
	}

	if delete {
		if !skipPrompt {
			// TODO: prompt for deletion
			fmt.Println("DELETE THE ABOVE BLOCKS? (Y/N)")
			if !askConfirmation() {
				sybil.Debug("ABORTING")
				return nil
			}

		}

		sybil.Debug("DELETING CANDIDATE BLOCKS")
		for _, b := range toTrim {
			sybil.Debug("DELETING", b.Name)
			if len(b.Name) > 5 {
				os.RemoveAll(b.Name)
			} else {
				sybil.Debug("REFUSING TO DELETE", b.Name)
			}
		}
	}

	return nil
}
