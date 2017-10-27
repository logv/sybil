package sybil

import "bytes"
import "path"
import "fmt"
import "encoding/gob"
import "io/ioutil"
import "os"

import . "github.com/logv/sybil/src/lib/config"
import . "github.com/logv/sybil/src/lib/common"
import . "github.com/logv/sybil/src/lib/structs"
import . "github.com/logv/sybil/src/storage/file_locks"

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
	dirname := path.Join(*FLAGS.DIR, t.Name)
	filename := path.Join(dirname, fmt.Sprintf("%s.db", fname))
	backup := path.Join(dirname, fmt.Sprintf("%s.bak", fname))

	flagfile := path.Join(dirname, fmt.Sprintf("%s.db.exists", fname))

	// Create a backup file
	Copy(backup, filename)

	// Create an encoder and send a value.
	enc := gob.NewEncoder(&network)
	Debug("SAVING TABLE INFO", t.SavedTableInfo)
	err := enc.Encode(t.SavedTableInfo)

	if err != nil {
		Error("table info encode:", err)
	}

	Debug("SERIALIZED TABLE INFO", fname, "INTO ", network.Len(), "BYTES")

	tempfile, err := ioutil.TempFile(dirname, "info.db")
	if err != nil {
		Error("ERROR CREATING TEMP FILE FOR TABLE INFO", err)
	}

	_, err = network.WriteTo(tempfile)
	if err != nil {
		Error("ERROR SAVING TABLE INFO INTO TEMPFILE", err)
	}

	RenameAndMod(tempfile.Name(), filename)
	os.Create(flagfile)
}
