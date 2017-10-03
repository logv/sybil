package sybil

import (
	"os"
	"path"

	"github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/printer"
	. "github.com/logv/sybil/src/lib/structs"
)

func MakeDir(t *Table) {
	tabledir := path.Join(*config.FLAGS.DIR, t.Name)
	os.MkdirAll(tabledir, 0755)
}

func PrintRecords(t *Table, records RecordList) {
	for i := 0; i < len(records); i++ {
		PrintRecord(records[i])
	}
}
