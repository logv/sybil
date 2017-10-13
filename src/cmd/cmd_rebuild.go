package sybilCmd

import "flag"

import sybil "github.com/logv/sybil/src/lib"

func RunRebuildCmdLine() {
	ReplaceInfo := flag.Bool("replace", false, "Replace broken info.db if it exists")
	ForceUpdate := flag.Bool("force", false, "Force re-calculation of info.db, even if it exists")
	flag.Parse()

	if *sybil.FLAGS.Table == "" {
		flag.PrintDefaults()
		return
	}

	if *sybil.FLAGS.Profile {
		profile := sybil.RunProfiler()
		defer profile.Start().Stop()
	}

	t := sybil.GetTable(*sybil.FLAGS.Table)

	loaded := t.LoadTableInfo() && *ForceUpdate == false
	if loaded {
		sybil.Print("TABLE INFO ALREADY EXISTS, NOTHING TO REBUILD!")
		return
	}

	t.DeduceTableInfoFromBlocks()

	// TODO: prompt to see if this table info looks good and then write it to
	// original info.db
	if *ReplaceInfo == true {
		sybil.Print("REPLACING info.db WITH DATA COMPUTED ABOVE")
		lock := sybil.Lock{Table: t, Name: "info"}
		lock.ForceDeleteFile()
		t.SaveTableInfo("info")
	} else {
		sybil.Print("SAVING TO tempInfo.db")
		t.SaveTableInfo("tempInfo")
	}
}
