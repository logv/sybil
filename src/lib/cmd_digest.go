package sybil

import "flag"
import "log"

// appends records to our record input queue
// every now and then, we should pack the input queue into a column, though
func RunDigestCmdLine() {
	digest_file := flag.String("file", INGEST_DIR, "Name of block to digest")
	flag.Parse()

	if *f_TABLE == "" {
		flag.PrintDefaults()
		return
	}

	if *f_PROFILE && PROFILER_ENABLED {
		profile := RUN_PROFILER()
		defer profile.Start().Stop()
	}

	DELETE_BLOCKS_AFTER_QUERY = false

	t := GetTable(*f_TABLE)
	if t.LoadTableInfo() == false {
		log.Println("Warning: Couldn't read table info, exiting early")
		return
	}
	t.DigestRecords(*digest_file)
}
