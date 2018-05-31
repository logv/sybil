package sybil

import "fmt"
import "path"
import "bytes"
import "encoding/gob"
import "io/ioutil"
import "time"
import "os"

type RowSavedInt struct {
	Name  int16
	Value int64
}

type RowSavedStr struct {
	Name  int16
	Value string
}

type RowSavedSet struct {
	Name  int16
	Value []string
}

type SavedRecord struct {
	Ints []RowSavedInt
	Strs []RowSavedStr
	Sets []RowSavedSet
}

func (s SavedRecord) toRecord(skipOutliers bool, t *Table) *Record {
	r := Record{}
	r.Ints = IntArr{}
	r.Strs = StrArr{}
	r.SetMap = SetMap{}

	b := t.LastBlock
	t.LastBlock.RecordList = append(t.LastBlock.RecordList, &r)

	b.table = t
	r.block = &b

	maxKeyID := 0
	for _, v := range t.KeyTable {
		if maxKeyID <= int(v) {
			maxKeyID = int(v) + 1
		}
	}

	r.ResizeFields(int16(maxKeyID))

	for _, v := range s.Ints {
		r.Populated[v.Name] = INT_VAL
		r.Ints[v.Name] = IntField(v.Value)
		t.updateIntInfo(v.Name, v.Value, skipOutliers)
	}

	for _, v := range s.Strs {
		r.AddStrField(t.getStringForKey(int(v.Name)), v.Value)
	}

	for _, v := range s.Sets {
		r.AddSetField(t.getStringForKey(int(v.Name)), v.Value)
		r.Populated[v.Name] = SET_VAL
	}

	return &r
}

func (r Record) toSavedRecord() *SavedRecord {
	s := SavedRecord{}
	for k, v := range r.Ints {
		if r.Populated[k] == INT_VAL {
			s.Ints = append(s.Ints, RowSavedInt{int16(k), int64(v)})
		}
	}

	for k, v := range r.Strs {
		if r.Populated[k] == STR_VAL {
			col := r.block.GetColumnInfo(int16(k))
			strVal := col.getStringForVal(int32(v))
			s.Strs = append(s.Strs, RowSavedStr{int16(k), strVal})
		}
	}

	for k, v := range r.SetMap {
		if r.Populated[k] == SET_VAL {
			col := r.block.GetColumnInfo(int16(k))
			setVals := make([]string, len(v))
			for i, val := range v {
				setVals[i] = col.getStringForVal(int32(val))
			}
			s.Sets = append(s.Sets, RowSavedSet{int16(k), setVals})
		}
	}

	return &s

}

type SavedRecords struct {
	RecordList []*SavedRecord
}

func (t *Table) LoadSavedRecordsFromLog(filename string) []*SavedRecord {
	Debug("LOADING RECORDS FROM LOG", filename)
	var marshalledRecords []*SavedRecord

	// Create an encoder and send a value.
	err := decodeInto(filename, &marshalledRecords)

	if err != nil {
		Debug("ERROR LOADING INGESTION LOG", err)
	}

	return marshalledRecords
}

func (t *Table) LoadRecordsFromLog(filename string, loadSpec *LoadSpec) RecordList {
	var marshalledRecords []*SavedRecord

	// Create an encoder and send a value.
	err := decodeInto(filename, &marshalledRecords)
	if err != nil {
		Debug("ERROR LOADING INGESTION LOG", err)
	}

	ret := make(RecordList, len(marshalledRecords))

	for i, r := range marshalledRecords {
		ret[i] = r.toRecord(loadSpec != nil && loadSpec.SkipOutliers, t)
	}
	return ret

}

func (t *Table) AppendRecordsToLog(flags *FlagDefs, records RecordList, blockname string) {
	if len(records) == 0 {
		return
	}

	// TODO: fix this up, so that we don't
	ingestdir := path.Join(*flags.DIR, t.Name, INGEST_DIR)
	tempingestdir := path.Join(*flags.DIR, t.Name, TEMP_INGEST_DIR)

	os.MkdirAll(ingestdir, 0777)
	os.MkdirAll(tempingestdir, 0777)

	w, _ := ioutil.TempFile(tempingestdir, fmt.Sprintf("%s_", blockname))

	marshalledRecords := make([]*SavedRecord, len(records))
	for i, r := range records {
		marshalledRecords[i] = r.toSavedRecord()
	}

	var network bytes.Buffer // Stand-in for the network.

	Debug("SAVING RECORDS", len(marshalledRecords), "TO INGESTION LOG")

	// Create an encoder and send a value.
	enc := gob.NewEncoder(&network)
	err := enc.Encode(marshalledRecords)

	if err != nil {
		Error("encode:", err)
	}

	filename := fmt.Sprintf("%s.db", w.Name())
	basename := path.Base(filename)

	Debug("SERIALIZED INTO LOG", filename, network.Len(), "BYTES", "( PER RECORD", network.Len()/len(marshalledRecords), ")")

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
