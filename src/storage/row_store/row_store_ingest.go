package sybil

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/storage/encoders"
)

func LoadSavedRecordsFromLog(t *Table, filename string) []*SavedRecord {
	Debug("LOADING RECORDS FROM LOG", filename)
	var marshalled_records []*SavedRecord

	// Create an encoder and send a value.
	err := DecodeInto(filename, &marshalled_records)

	if err != nil {
		Debug("ERROR LOADING INGESTION LOG", err)
	}

	return marshalled_records
}

func AppendRecordsToLog(t *Table, records RecordList, blockname string) {
	if len(records) == 0 {
		return
	}

	// TODO: fix this up, so that we don't
	ingestdir := path.Join(*FLAGS.DIR, t.Name, INGEST_DIR)
	tempingestdir := path.Join(*FLAGS.DIR, t.Name, TEMP_INGEST_DIR)

	os.MkdirAll(ingestdir, 0777)
	os.MkdirAll(tempingestdir, 0777)

	w, err := ioutil.TempFile(tempingestdir, fmt.Sprintf("%s_", blockname))

	marshalled_records := make([]*SavedRecord, len(records))
	for i, r := range records {
		marshalled_records[i] = ToSavedRecord(r)
	}

	var network bytes.Buffer // Stand-in for the network.

	Debug("SAVING RECORDS", len(marshalled_records), "TO INGESTION LOG")

	// Create an encoder and send a value.
	enc := gob.NewEncoder(&network)
	err = enc.Encode(marshalled_records)

	if err != nil {
		Error("encode:", err)
	}

	filename := fmt.Sprintf("%s.db", w.Name())
	basename := path.Base(filename)

	Debug("SERIALIZED INTO LOG", filename, network.Len(), "BYTES", "( PER RECORD", network.Len()/len(marshalled_records), ")")

	network.WriteTo(w)

	for i := 0; i < 3; i++ {
		fullname := path.Join(ingestdir, basename)
		// need to keep re-trying, right?
		err = RenameAndMod(w.Name(), fullname)
		if err == nil {
			// we are done writing, time to exit
			return
		}

		if err != nil {
			time.Sleep(time.Millisecond * 10)
		}
	}

	Warn("COULDNT INGEST INTO ROW STORE")
}
