package cmd

import "flag"

import sybil "github.com/logv/sybil/src/lib"

func RunRebuildCmdLine() {
	REPLACE_INFO := flag.Bool("replace", false, "Replace broken info.db if it exists")
	FORCE_UPDATE := flag.Bool("force", false, "Force re-calculation of info.db, even if it exists")
	flag.Parse()

	if *sybil.FLAGS.TABLE == "" {
		flag.PrintDefaults()
		return
	}

	if *sybil.FLAGS.PROFILE {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	t := sybil.GetTable(*sybil.FLAGS.TABLE)

	loaded := t.LoadTableInfo() && *FORCE_UPDATE == false
	if loaded {
		sybil.Print("TABLE INFO ALREADY EXISTS, NOTHING TO REBUILD!")
		return
	}

	t.DeduceTableInfoFromBlocks()

	// TODO: prompt to see if this table info looks good and then write it to
	// original info.db
	if *REPLACE_INFO == true {
		sybil.Print("REPLACING info.db WITH DATA COMPUTED ABOVE")
		lock := sybil.Lock{Table: t, Name: "info"}
		lock.ForceDeleteFile()
		t.SaveTableInfo("info")
	} else {
		sybil.Print("SAVING TO temp_info.db")
		t.SaveTableInfo("temp_info")
	}
}
