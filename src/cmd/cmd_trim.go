package sybil_cmd

import "flag"
import "log"
import "fmt"

import sybil "github.com/logV/sybil/src/lib"

func RunTrimCmdLine() {
	MB_LIMIT := flag.Int("mb", 0, "max table size in MB")
	DELETE_BEFORE := flag.Int("before", 0, "delete blocks with data older than TIMESTAMP")
	sybil.FLAGS.TIME_COL = flag.String("time-col", "", "which column to treat as a timestamp [REQUIRED]")
	flag.Parse()

	if *sybil.FLAGS.TABLE == "" || *sybil.FLAGS.TIME_COL == "" {
		flag.PrintDefaults()
		return
	}

	if *sybil.FLAGS.PROFILE {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	sybil.DELETE_BLOCKS_AFTER_QUERY = false

	t := sybil.GetTable(*sybil.FLAGS.TABLE)
	if t.LoadTableInfo() == false {
		log.Println("Warning: Couldn't read table info, exiting early")
		return
	}

	loadSpec := t.NewLoadSpec()
	loadSpec.Int(*sybil.FLAGS.TIME_COL)

	trimSpec := sybil.TrimSpec{}
	trimSpec.DeleteBefore = int64(*DELETE_BEFORE)
	trimSpec.MBLimit = int64(*MB_LIMIT)

	to_trim := t.TrimTable(&trimSpec)
	if len(to_trim) > 0 {
		for _, b := range to_trim {
			fmt.Println(b.Name)
		}
	}
}
