package sybil

import "bytes"
import "path"
import "fmt"
import "encoding/gob"
import "io/ioutil"
import "os"

import "github.com/logv/sybil/src/lib/config"
import "github.com/logv/sybil/src/lib/common"
import . "github.com/logv/sybil/src/lib/structs"
import . "github.com/logv/sybil/src/lib/metadata"
import . "github.com/logv/sybil/src/lib/locks"

func SetKeyType(t *Table, NameID int16, col_type int8) bool {
	cur_type, ok := t.KeyTypes[NameID]
	if !ok {
		t.KeyTypes[NameID] = col_type
	} else {
		if cur_type != col_type {
			common.Debug("TABLE", t.KeyTable)
			common.Debug("TYPES", t.KeyTypes)
			common.Warn("trying to over-write column type for key ", NameID, GetTableStringForKey(t, int(NameID)), " OLD TYPE", cur_type, " NEW TYPE", col_type)
			return false
		}
	}

	return true

}

func SaveTableInfo(t *Table, fname string) {
	save_table := getSaveTable(t)
	saveTableInfo(save_table, fname)

}

func getSaveTable(t *Table) *Table {
	return &Table{SavedTableInfo: SavedTableInfo{Name: t.Name,
		KeyTable: t.KeyTable,
		KeyTypes: t.KeyTypes,
		IntInfo:  t.IntInfo,
		StrInfo:  t.StrInfo}}
}

func saveTableInfo(t *Table, fname string) {
	if GrabInfoLock(t) == false {
		return
	}

	defer ReleaseInfoLock(t)
	var network bytes.Buffer // Stand-in for the network.
	dirname := path.Join(*config.FLAGS.DIR, t.Name)
	filename := path.Join(dirname, fmt.Sprintf("%s.db", fname))
	backup := path.Join(dirname, fmt.Sprintf("%s.bak", fname))

	flagfile := path.Join(dirname, fmt.Sprintf("%s.db.exists", fname))

	// Create a backup file
	common.Copy(backup, filename)

	// Create an encoder and send a value.
	enc := gob.NewEncoder(&network)
	common.Debug("SAVING TABLE INFO", t.SavedTableInfo)
	err := enc.Encode(t.SavedTableInfo)

	if err != nil {
		common.Error("table info encode:", err)
	}

	common.Debug("SERIALIZED TABLE INFO", fname, "INTO ", network.Len(), "BYTES")

	tempfile, err := ioutil.TempFile(dirname, "info.db")
	if err != nil {
		common.Error("ERROR CREATING TEMP FILE FOR TABLE INFO", err)
	}

	_, err = network.WriteTo(tempfile)
	if err != nil {
		common.Error("ERROR SAVING TABLE INFO INTO TEMPFILE", err)
	}

	common.RenameAndMod(tempfile.Name(), filename)
	os.Create(flagfile)
}
