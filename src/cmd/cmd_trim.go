package cmd

import (
	"flag"
	"fmt"
	"os"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/query/specs"
	. "github.com/logv/sybil/src/storage/metadata_io"
	. "github.com/logv/sybil/src/utils/table_trim"
)

func askConfirmation() bool {

	var response string
	_, err := fmt.Scanln(&response)
	if err != nil {
		Error(err)
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

	FLAGS.TIME_COL = flag.String("time-col", "", "which column to treat as a timestamp [REQUIRED]")
	flag.Parse()

	if *FLAGS.TABLE == "" || *FLAGS.TIME_COL == "" {
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

	loadSpec := NewTableLoadSpec(t)
	loadSpec.Int(*FLAGS.TIME_COL)

	trimSpec := TrimSpec{}
	trimSpec.DeleteBefore = int64(*DELETE_BEFORE)
	trimSpec.MBLimit = int64(*MB_LIMIT)

	to_trim := TrimTable(t, &trimSpec)

	Debug("FOUND", len(to_trim), "CANDIDATE BLOCKS FOR TRIMMING")
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
				Debug("ABORTING")
				return
			}

		}

		Debug("DELETING CANDIDATE BLOCKS")
		for _, b := range to_trim {
			Debug("DELETING", b.Name)
			if len(b.Name) > 5 {
				os.RemoveAll(b.Name)
			} else {
				Debug("REFUSING TO DELETE", b.Name)
			}
		}

	}
}
