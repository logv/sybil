package cmd

import (
	"flag"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/structs"
	flock "github.com/logv/sybil/src/storage/file_locks"
	md_io "github.com/logv/sybil/src/storage/metadata_io"
	. "github.com/logv/sybil/src/utils/table_info_recover"
)

func RunRebuildCmdLine() {
	REPLACE_INFO := flag.Bool("replace", false, "Replace broken info.db if it exists")
	FORCE_UPDATE := flag.Bool("force", false, "Force re-calculation of info.db, even if it exists")
	flag.Parse()

	if *FLAGS.TABLE == "" {
		flag.PrintDefaults()
		return
	}

	if *FLAGS.PROFILE {
		profile := RUN_PROFILER()
		defer profile.Start().Stop()
	}

	t := GetTable(*FLAGS.TABLE)

	loaded := md_io.LoadTableInfo(t) && *FORCE_UPDATE == false
	if loaded {
		Print("TABLE INFO ALREADY EXISTS, NOTHING TO REBUILD!")
		return
	}

	DeduceTableInfoFromBlocks(t)

	// TODO: prompt to see if this table info looks good and then write it to
	// original info.db
	if *REPLACE_INFO == true {
		Print("REPLACING info.db WITH DATA COMPUTED ABOVE")
		lock := flock.Lock{Table: t, Name: "info"}
		lock.ForceDeleteFile()
		md_io.SaveTableInfo(t, "info")
	} else {
		Print("SAVING TO temp_info.db")
		md_io.SaveTableInfo(t, "temp_info")
	}
}
