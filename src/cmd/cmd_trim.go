package cmd

import (
	"flag"
	"fmt"
	"os"

	sybil "github.com/logv/sybil/src/lib"
	"github.com/logv/sybil/src/lib/common"
)

func askConfirmation() bool {

	var response string
	_, err := fmt.Scanln(&response)
	if err != nil {
		common.Error(err)
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

	common.FLAGS.TIME_COL = flag.String("time-col", "", "which column to treat as a timestamp [REQUIRED]")
	flag.Parse()

	if *common.FLAGS.TABLE == "" || *common.FLAGS.TIME_COL == "" {
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

	loadSpec := t.NewLoadSpec()
	loadSpec.Int(*common.FLAGS.TIME_COL)

	trimSpec := sybil.TrimSpec{}
	trimSpec.DeleteBefore = int64(*DELETE_BEFORE)
	trimSpec.MBLimit = int64(*MB_LIMIT)

	to_trim := t.TrimTable(&trimSpec)

	common.Debug("FOUND", len(to_trim), "CANDIDATE BLOCKS FOR TRIMMING")
	if len(to_trim) > 0 {
		for _, b := range to_trim {
			fmt.Println(b.Name)
		}
	}

	if *DELETE {
		if *REALLY != true {
			// TODO: prompt for deletion
			fmt.Println("DELETE THE ABOVE BLOCKS? (Y/N)")
			if askConfirmation() == false {
				common.Debug("ABORTING")
				return
			}

		}

		common.Debug("DELETING CANDIDATE BLOCKS")
		for _, b := range to_trim {
			common.Debug("DELETING", b.Name)
			if len(b.Name) > 5 {
				os.RemoveAll(b.Name)
			} else {
				common.Debug("REFUSING TO DELETE", b.Name)
			}
		}

	}
}
