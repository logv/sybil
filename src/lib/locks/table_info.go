package locks

import "time"
import "path"
import "github.com/logv/sybil/src/lib/config"
import "github.com/logv/sybil/src/lib/common"
import . "github.com/logv/sybil/src/lib/structs"
import . "github.com/logv/sybil/src/lib/metadata"
import . "github.com/logv/sybil/src/lib/encoders"

func LoadTableInfo(t *Table) bool {
	tablename := t.Name
	filename := path.Join(*config.FLAGS.DIR, tablename, "info.db")
	if GrabInfoLock(t) {
		defer ReleaseInfoLock(t)
	} else {
		common.Debug("LOAD TABLE INFO LOCK TAKEN")
		return false
	}

	return LoadTableInfoFrom(t, filename)
}

func LoadTableInfoFrom(t *Table, filename string) bool {
	saved_table := Table{}
	saved_table.Name = t.Name

	InitDataStructures(&saved_table)

	start := time.Now()

	common.Debug("OPENING TABLE INFO FROM FILENAME", filename)
	err := DecodeInto(filename, &saved_table)
	end := time.Now()
	if err != nil {
		common.Debug("TABLE INFO DECODE:", err)
		return false
	}

	if DEBUG_TIMING {
		common.Debug("TABLE INFO OPEN TOOK", end.Sub(start))
	}

	if len(saved_table.KeyTable) > 0 {
		t.KeyTable = saved_table.KeyTable
	}

	if len(saved_table.KeyTypes) > 0 {
		t.KeyTypes = saved_table.KeyTypes
	}

	if saved_table.IntInfo != nil {
		t.IntInfo = saved_table.IntInfo
	}
	if saved_table.StrInfo != nil {
		t.StrInfo = saved_table.StrInfo
	}

	// If we are recovering the INFO lock, we won't necessarily have
	// all fields filled out
	if t.StringIDMutex != nil {
		PopulateStringIDLookup(t)
	}

	return true
}
