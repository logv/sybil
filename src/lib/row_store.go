package sybil

import "log"
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

func (s SavedRecord) toRecord(t *Table) *Record {
	r := Record{}
	r.Ints = IntArr{}
	r.Strs = StrArr{}
	r.SetMap = SetMap{}

	b := t.LastBlock
	t.LastBlock.RecordList = append(t.LastBlock.RecordList, &r)

	b.table = t
	r.block = &b

	for _, v := range s.Ints {
		r.ResizeFields(v.Name)
		r.Populated[v.Name] = INT_VAL
		r.Ints[v.Name] = IntField(v.Value)
		t.update_int_info(v.Name, v.Value)
	}

	for _, v := range s.Strs {
		r.ResizeFields(v.Name)
		r.AddStrField(t.get_string_for_key(int(v.Name)), v.Value)
	}

	for _, v := range s.Sets {
		r.ResizeFields(v.Name)

		r.AddSetField(t.get_string_for_key(int(v.Name)), v.Value)
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
			str_val := col.get_string_for_val(int32(v))
			s.Strs = append(s.Strs, RowSavedStr{int16(k), str_val})
		}
	}

	for k, v := range r.SetMap {
		if r.Populated[k] == SET_VAL {
			col := r.block.GetColumnInfo(int16(k))
			set_vals := make([]string, len(v))
			for i, val := range v {
				set_vals[i] = col.get_string_for_val(int32(val))
			}
			s.Sets = append(s.Sets, RowSavedSet{int16(k), set_vals})
		}
	}

	return &s

}

type SavedRecords struct {
	RecordList []*SavedRecord
}

func (t *Table) LoadSavedRecordsFromLog(filename string) []*SavedRecord {
	log.Println("LOADING RECORDS FROM LOG", filename)
	var marshalled_records []*SavedRecord

	file, err := os.Open(filename)

	if err != nil {
		log.Println("ERROR OPENING FILE", filename, err)
	}

	// Create an encoder and send a value.
	enc := gob.NewDecoder(file)
	err = enc.Decode(&marshalled_records)

	if err != nil {
		log.Println("ERROR LOADING INGESTION LOG", err)
	}

	return marshalled_records
}

func (t *Table) LoadRecordsFromLog(filename string) RecordList {
	var marshalled_records []*SavedRecord

	file, err := os.Open(filename)

	if err != nil {
		log.Println("ERROR OPENING FILE", filename, err)
	}

	// Create an encoder and send a value.
	enc := gob.NewDecoder(file)
	err = enc.Decode(&marshalled_records)

	if err != nil {
		log.Println("ERROR LOADING INGESTION LOG", err)
	}

	ret := make(RecordList, len(marshalled_records))

	for i, r := range marshalled_records {
		ret[i] = r.toRecord(t)
	}

	return ret
}

func (t *Table) AppendRecordsToLog(records RecordList, blockname string) {
	if len(records) == 0 {
		return
	}

	// TODO: fix this up, so that we don't
	ingestdir := path.Join(*f_DIR, t.Name, INGEST_DIR)
	tempingestdir := path.Join(*f_DIR, t.Name, TEMP_INGEST_DIR)
	digestdir := path.Join(*f_DIR, t.Name, STOMACHE_DIR)

	os.MkdirAll(ingestdir, 0777)
	os.MkdirAll(tempingestdir, 0777)

	w, err := ioutil.TempFile(tempingestdir, fmt.Sprintf("%s_", blockname))

	marshalled_records := make([]*SavedRecord, len(records))
	for i, r := range records {
		marshalled_records[i] = r.toSavedRecord()
	}

	var network bytes.Buffer // Stand-in for the network.

	log.Println("SAVING RECORDS", len(marshalled_records), "TO INGESTION LOG")

	// Create an encoder and send a value.
	enc := gob.NewEncoder(&network)
	err = enc.Encode(marshalled_records)

	if err != nil {
		log.Fatal("encode:", err)
	}

	filename := fmt.Sprintf("%s.db", w.Name())
	basename := path.Base(filename)

	log.Println("SERIALIZED INTO LOG", filename, network.Len(), "BYTES", "( PER RECORD", network.Len()/len(marshalled_records), ")")

	network.WriteTo(w)

	for i := 0; i < 3; i++ {
		fullname := path.Join(ingestdir, basename)
		// need to keep re-trying, right?
		err = os.Rename(w.Name(), fullname)
		if err == nil {
			break
		}

		if err != nil {
			log.Println("WARNING: COULDNT RENAME TEMP INGEST BLOCK!", fullname, err)
			log.Println("TRYING TO PLACE RECORDS IN DIGEST DIR")
			fullname := path.Join(digestdir, basename)
			err = os.Rename(w.Name(), fullname)
			if err == nil {
				break
			}

			time.Sleep(time.Millisecond * 10)
		}
	}
}
