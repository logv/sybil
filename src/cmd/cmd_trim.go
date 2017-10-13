package sybilCmd

import "flag"

import "fmt"
import "os"

import sybil "github.com/logv/sybil/src/lib"

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
	MbLimit := flag.Int("mb", 0, "max table size in MB")
	DeleteBefore := flag.Int("before", 0, "delete blocks with data older than TIMESTAMP")
	DELETE := flag.Bool("delete", false, "delete blocks? be careful! will actually delete your data!")
	REALLY := flag.Bool("really", false, "don't prompt before deletion")

	sybil.FLAGS.TimeCol = flag.String("time-col", "", "which column to treat as a timestamp [REQUIRED]")
	flag.Parse()

	if *sybil.FLAGS.Table == "" || *sybil.FLAGS.TimeCol == "" {
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

	loadSpec := t.NewLoadSpec()
	loadSpec.Int(*sybil.FLAGS.TimeCol)

	trimSpec := sybil.TrimSpec{}
	trimSpec.DeleteBefore = int64(*DeleteBefore)
	trimSpec.MBLimit = int64(*MbLimit)

	toTrim := t.TrimTable(&trimSpec)

	sybil.Debug("FOUND", len(toTrim), "CANDIDATE BLOCKS FOR TRIMMING")
	if len(toTrim) > 0 {
		for _, b := range toTrim {
			fmt.Println(b.Name)
		}
	}

	if *DELETE {
		if *REALLY != true {
			// TODO: prompt for deletion
			fmt.Println("DELETE THE ABOVE BLOCKS? (Y/N)")
			if askConfirmation() == false {
				sybil.Debug("ABORTING")
				return
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
}
