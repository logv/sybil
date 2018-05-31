package cmd

import (
	"flag"

	"github.com/logv/sybil/src/sybil"
)

func RunRebuildCmdLine() {
	REPLACE_INFO := flag.Bool("replace", false, "Replace broken info.db if it exists")
	FORCE_UPDATE := flag.Bool("force", false, "Force re-calculation of info.db, even if it exists")
	flags := sybil.DefaultFlags()
	flag.Parse()

	if *flags.TABLE == "" {
		flag.PrintDefaults()
		return
	}

	if *flags.PROFILE {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	t := sybil.GetTable(*flags.TABLE)

	loaded := t.LoadTableInfo(flags) && !*FORCE_UPDATE
	if loaded {
		sybil.Print("TABLE INFO ALREADY EXISTS, NOTHING TO REBUILD!")
		return
	}

	t.DeduceTableInfoFromBlocks(flags)

	// TODO: prompt to see if this table info looks good and then write it to
	// original info.db
	if *REPLACE_INFO {
		sybil.Print("REPLACING info.db WITH DATA COMPUTED ABOVE")
		lock := sybil.Lock{Table: t, Name: "info"}
		lock.ForceDeleteFile(flags)
		t.SaveTableInfo(flags, "info")
	} else {
		sybil.Print("SAVING TO temp_info.db")
		t.SaveTableInfo(flags, "temp_info")
	}
}
