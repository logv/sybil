package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
)

// {{{ INITIALIZERS
func SetBinPath(binPath string) {
	// TODO: validate this path actually exists
	SYBIL_BIN = binPath
}

var DEBUG_FLUSH = false

// }}}

// {{{ INGESTION
// We ingest JSON records by unmarshalling them into an interface{}, then
// we place the interface{} into out table's append log
func (t *SybilTable) AddJSONRecords(records [][]byte) {
	for _, r := range records {
		var unmarshalled interface{}
		err := json.Unmarshal(r, &unmarshalled)
		if err != nil {
			Print("ERROR UNPACKING JSON RECORD", err)
		} else {
			t.NewRecords = append(t.NewRecords, unmarshalled)
		}
	}

}

// this API lets you ingest records as arbitrary interfaces
// it validates that the interface{} can be marshalled into JSON before
// placing the interface into the actual record list
func (t *SybilTable) AddStructRecords(records []interface{}) {
	for _, r := range records {
		_, err := json.Marshal(r)
		if err != nil {
			Debug("DISCARDING RECORD", r, "BECAUSE", err)
		} else {
			t.NewRecords = append(t.NewRecords, r)
		}
	}
}

func (t *SybilTable) AddMapRecords(records []map[string]interface{}) {
	for _, r := range records {
		_, err := json.Marshal(r)
		if err != nil {
			Debug("DISCARDING RECORD", r, "BECAUSE", err)
		} else {
			t.NewRecords = append(t.NewRecords, r)
		}
	}

}

func (t *SybilTable) AddSybilMapRecords(records []SybilMapRecord) {
	for _, r := range records {
		_, err := json.Marshal(r)
		if err != nil {
			Debug("DISCARDING RECORD", r, "BECAUSE", err)
		} else {
			t.NewRecords = append(t.NewRecords, r)
		}
	}

}

func (t *SybilTable) AddRecords(records interface{}) {
	fmt.Printf("ADDING RECORDS OF TYPE: %T\n", records)
	switch v := records.(type) {
	case [][]byte:
		t.AddJSONRecords(v)
	case []interface{}:
		t.AddStructRecords(v)
	case []SybilMapRecord:
		t.AddSybilMapRecords(v)
	case []map[string]interface{}:
		t.AddMapRecords(v)
	default:
		Error(fmt.Sprintf("COULDNT FIGURE OUT HOW TO INGEST RECORDS OF TYPE %T", records))
	}
}

// This API will actually record sybil records into a sybil table
func (t *SybilTable) FlushRecords() {

	flags := []string{"ingest", "-table", t.Config.Table, "-dir", t.Config.Dir, "-save-srb"}
	cmd := exec.Command(SYBIL_BIN, flags...)

	jsonBytes := make([][]byte, 0)
	for _, r := range t.NewRecords {
		b, err := json.Marshal(r)
		if err == nil {
			jsonBytes = append(jsonBytes, b)
		} else {
			Debug("COULDNT FORMAT RECORD", r)
		}
	}

	joinedBytes := bytes.Join(jsonBytes, []byte("\n"))
	cmd.Stdin = bytes.NewReader(joinedBytes)
	count := 0
	for _, r := range jsonBytes {
		var unmarshalled interface{}
		err := json.Unmarshal(r, &unmarshalled)
		if err != nil {
			Print("ERROR UNMARSHALLING JSON RECORD", err)
		} else {
			count++
		}

		if DEBUG_FLUSH {
			Debug("FLUSHING RECORD: ", unmarshalled)
		}
	}

	out, err := cmd.Output()
	if err != nil {
		Error("CAN'T FLUSH RECORDS!", out, err)
	} else {
		Debug("SUCCESSFULLY FLUSHED ", count, " RECORDS TO ", t.Config.Table)
		t.NewRecords = make([]interface{}, 0)
	}

}

// }}} INGESTION

// vim: foldmethod=marker
