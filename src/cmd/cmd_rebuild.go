package cmd

import (
	"flag"

	sybil "github.com/logv/sybil/src/lib"
	"github.com/logv/sybil/src/lib/common"
	"github.com/logv/sybil/src/lib/config"
)

func RunRebuildCmdLine() {
	REPLACE_INFO := flag.Bool("replace", false, "Replace broken info.db if it exists")
	FORCE_UPDATE := flag.Bool("force", false, "Force re-calculation of info.db, even if it exists")
	flag.Parse()

	if *config.FLAGS.TABLE == "" {
		flag.PrintDefaults()
		return
	}

	if *config.FLAGS.PROFILE {
		profile := config.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	t := sybil.GetTable(*config.FLAGS.TABLE)

	loaded := t.LoadTableInfo() && *FORCE_UPDATE == false
	if loaded {
		common.Print("TABLE INFO ALREADY EXISTS, NOTHING TO REBUILD!")
		return
	}

	t.DeduceTableInfoFromBlocks()

	// TODO: prompt to see if this table info looks good and then write it to
	// original info.db
	if *REPLACE_INFO == true {
		common.Print("REPLACING info.db WITH DATA COMPUTED ABOVE")
		lock := sybil.Lock{Table: t, Name: "info"}
		lock.ForceDeleteFile()
		t.SaveTableInfo("info")
	} else {
		common.Print("SAVING TO temp_info.db")
		t.SaveTableInfo("temp_info")
	}
}
