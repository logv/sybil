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
type RowSavedFloat struct {
	Name  int16
	Value float64
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
	Ints   []RowSavedInt
	Strs   []RowSavedStr
	Sets   []RowSavedSet
	Floats []RowSavedFloat
}

type SavedRecordBlock struct {
	RecordList   []*SavedRecord
	KeyTable     *map[string]int16
	max_key_id   int
	key_exchange map[int16]int16
}

func (srb *SavedRecordBlock) init_data_structures(t *Table) {
	key_exchange := make(map[int16]int16, 0)
	max_key_id := 0
	for k, v := range *srb.KeyTable {
		vv := t.get_key_id(k)
		if max_key_id <= int(vv) {
			max_key_id = int(vv) + 1
		}

		key_exchange[v] = vv
	}

	srb.key_exchange = key_exchange
	srb.max_key_id = max_key_id
}

func get_short_key_id(t *Table, key_exchange map[int16]int16, key_id int16) int16 {
	if t.ShortKeyInfo == nil || t.ShortKeyInfo.KeyExchange == nil {
		return key_id
	}
	key_id, ok := key_exchange[key_id]
	if !ok {
		return -1
	}

	return key_id

}

func (srb *SavedRecordBlock) toRecord(t *Table, s *SavedRecord) *Record {
	// SITUATION:
	// the saved record is saved as integers arrays which align either with the
	// srb.KeyTable (if using save-srb flag) or with the table's KeyTable.
	// When we use --shorten-key-table we remap the table's KeyTable though, so
	// we need to actually re-do the layout of the SavedRecord to map to the
	// modified table's KeyTable.

	// if we don't, we end up with SavedRecords having the wrong indeces and the column types
	// will be incorrect
	// SOLUTION:
	// when using a shortened key table, we have to build a keyxchange for remapping SavedRecords
	// this means we have two key exchange tables: one for SRB and one for Shortened Key Tables.
	// the reason we have SRB exchange is for gouthamve's use case of generating
	// many new columns on the fly.
	// so in the end, what we do is:
	// first we do the SRB <-> KeyTable exchange (using srb.key_exchange), then we do get_short_key_id
	// on the resulting key in case we are using a shortened key table during querying
	r := Record{}
	r.Ints = IntArr{}
	r.Strs = StrArr{}
	r.SetMap = SetMap{}

	b := t.LastBlock
	t.LastBlock.RecordList = append(t.LastBlock.RecordList, &r)

	b.table = t
	r.block = &b

	max_key_id := int16(srb.max_key_id)
	key_exchange := srb.key_exchange
	r.ResizeFields(int16(max_key_id))

	var key_id int
	for _, v := range s.Ints {
		key_id = int(get_short_key_id(t, key_exchange, v.Name))
		if key_id == -1 {
			continue
		}

		r.AddIntField(t.get_string_for_key(key_id), v.Value)
	}
	for _, v := range s.Floats {
		key_id = int(get_short_key_id(t, key_exchange, v.Name))
		if key_id == -1 {
			continue
		}

		r.AddFloatField(t.get_string_for_key(key_id), v.Value)
	}

	for _, v := range s.Strs {
		key_id = int(get_short_key_id(t, key_exchange, v.Name))
		if key_id == -1 {
			continue
		}

		r.AddStrField(t.get_string_for_key(key_id), v.Value)
	}

	for _, v := range s.Sets {
		key_id = int(get_short_key_id(t, key_exchange, v.Name))
		if key_id == -1 {
			continue
		}
		r.AddSetField(t.get_string_for_key(key_id), v.Value)
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

	for k, v := range r.Floats {
		if r.Populated[k] == FLOAT_VAL {
			s.Floats = append(s.Floats, RowSavedFloat{int16(k), float64(v)})
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

func (t *Table) LoadRecordsFromLog(filename string) RecordList {
	var srb SavedRecordBlock

	err := decodeInto(filename, &srb)
	if err != nil {
		err := decodeInto(filename, &srb.RecordList)
		if err != nil {
			Debug("ERROR LOADING INGESTION LOG", err)
		}
	}

	ret := make(RecordList, len(srb.RecordList))

	// If the KeyTable doesn't exist, it means we are loading old records that
	// were ingested without a keytable
	if srb.KeyTable == nil {
		srb.KeyTable = get_key_table(t)
	}

	srb.init_data_structures(t)
	for i, r := range srb.RecordList {
		ret[i] = srb.toRecord(t, r)
	}

	return ret

}

func get_key_table(t *Table) *map[string]int16 {
	if t.AllKeyInfo != nil {
		return &t.AllKeyInfo.KeyTable
	}
	return &t.KeyTable
}

func (t *Table) AppendRecordsToLog(records RecordList, blockname string) {
	if len(records) == 0 {
		return
	}

	// TODO: fix this up, so that we don't
	ingestdir := path.Join(FLAGS.DIR, t.Name, INGEST_DIR)
	tempingestdir := path.Join(FLAGS.DIR, t.Name, TEMP_INGEST_DIR)

	os.MkdirAll(ingestdir, 0777)
	os.MkdirAll(tempingestdir, 0777)

	w, err := ioutil.TempFile(tempingestdir, fmt.Sprintf("%s_", blockname))

	marshalled_records := make([]*SavedRecord, len(records))
	for i, r := range records {
		marshalled_records[i] = r.toSavedRecord()
	}

	var network bytes.Buffer // Stand-in for the network.

	Debug("SAVING RECORDS", len(marshalled_records), "TO INGESTION LOG")

	// Create an encoder and send a value.
	enc := gob.NewEncoder(&network)

	if FLAGS.SAVE_AS_SRB {
		Debug("SAVING INTO SRB")
		srb := SavedRecordBlock{}
		srb.RecordList = marshalled_records
		srb.KeyTable = get_key_table(t)
		err = enc.Encode(srb)
	} else {
		err = enc.Encode(marshalled_records)
	}

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
