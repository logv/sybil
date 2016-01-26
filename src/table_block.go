package edb

import "sync"
import "fmt"
import "bytes"
import "log"
import "os"
import "encoding/gob"
import "strings"

// Table Block should have a bunch of metadata next to it, too
type TableBlock struct {
  RecordList []*Record
  StringTable map[string]int32 // String Value lookup

  string_id_m *sync.Mutex
  val_string_id_lookup map[int32]string
  table *Table

  columns map[int16]*TableColumn

}

func newTableBlock() TableBlock {

  tb := TableBlock{}
  tb.StringTable = make(map[string]int32)
  tb.columns = make(map[int16]*TableColumn)
  tb.val_string_id_lookup = make(map[int32]string)
  tb.string_id_m = &sync.Mutex{}

  return tb

}

type SavedBlock struct {
  StringTable map[string]int32 // String Value lookup
  Records []*SavedRecord
}

func (tb *TableBlock) get_key_id(name string) int16 {
  return tb.table.get_key_id(name)
}

func (tb *TableBlock) get_string_for_key(id int16) string {
  return tb.table.get_string_for_key(id)

}

type ValueMap map[int32][]int32
type SetMap map[int32][]int32

func record_value(same_map map[int16]ValueMap, index int32, name int16, value int32) {
  s, ok := same_map[name]
  if !ok {
    same_map[name] = ValueMap{}
    s = same_map[name]
  }

  vi := int32(value)

  s[vi] = append(s[vi], int32(index))
}

func (tb *TableBlock) SaveToColumns(filename string) {
  records := tb.RecordList
  // making a cross section of records that share values
  // goes from fieldname{} -> value{} -> record
  same_ints := make(map[int16]ValueMap)
  same_strs := make(map[int16]ValueMap)
  same_sets := make(map[int16]SetMap)

  for i, r := range records {
    for k, v := range r.Ints {
      record_value(same_ints, int32(i), k, int32(v))
    }
    for k, v := range r.Strs {
      // TODO: transition str id to the per column ID?
      v_name := r.block.columns[k].get_string_for_val(int32(v))
      v_id := tb.columns[k].get_val_id(v_name)

      // record the transitioned key
      record_value(same_strs, int32(i), k, int32(v_id))
    }
    for k, v := range r.Sets {
      s, ok := same_sets[k]
      if !ok {
        s = SetMap{}
        same_sets[k] = s
      }
      s[int32(i)] = v
    }
  }


  // now make the dir and shoot each blob out into a separate file

  dirname := strings.Replace(filename, ".db", "", 1)
  // SAVED TO A SINGLE BLOCK ON DISK, NOW TO SAVE IT OUT TO SEPARATE VALUES
  os.MkdirAll(dirname, 0777)
  for k, v := range same_ints {
    intCol := SavedInts{}
    for bucket, records := range v {
      si := SavedIntColumn{Name: k, Value: bucket, Records: records}
      intCol.Bins = append(intCol.Bins, si)
    }

    col_fname := fmt.Sprintf("%s/int_%s.db", dirname, tb.get_string_for_key(k))

    var network bytes.Buffer // Stand-in for the network.

    // Create an encoder and send a value.
    enc := gob.NewEncoder(&network)
    err := enc.Encode(intCol)

    if err != nil {
      log.Fatal("encode:", err)
    }

    fmt.Println(k, "SERIALIZED INTO COLUMN BLOCK", col_fname, network.Len(), "BYTES", "( PER RECORD", network.Len() / len(records), ")");

    w, _ := os.Create(col_fname)
    network.WriteTo(w);
  }


  for k, v := range same_strs {
    strCol := SavedStrs{}
    temp_block := newTableBlock()

    for bucket, records := range v {
      str_id := temp_block.columns[k].get_val_id(tb.columns[k].get_string_for_val(bucket))

      si := SavedStrColumn{Name: k, Value: str_id, Records: records}
      strCol.Bins = append(strCol.Bins, si)
    }

    strCol.StringTable = temp_block.StringTable

    col_fname := fmt.Sprintf("%s/str_%s.db", dirname, tb.get_string_for_key(k))

    var network bytes.Buffer // Stand-in for the network.

    // Create an encoder and send a value.
    enc := gob.NewEncoder(&network)
    err := enc.Encode(strCol)

    if err != nil {
      log.Fatal("encode:", err)
    }

    fmt.Println(k, "SERIALIZED INTO COLUMN BLOCK", col_fname, network.Len(), "BYTES", "( PER RECORD", network.Len() / len(records), ")");

    w, _ := os.Create(col_fname)
    network.WriteTo(w);

  }




}


func (tb *TableBlock) SaveToFile(filename string) {
  records := tb.RecordList

  marshalled_records := make([]*SavedRecord, len(records))
  saved_block := SavedBlock{Records: marshalled_records}
  for i, r := range records {
    marshalled_records[i] = r.toSavedRecord(tb)
  }
  saved_block.StringTable = tb.StringTable


  var network bytes.Buffer // Stand-in for the network.

  // Create an encoder and send a value.
  enc := gob.NewEncoder(&network)
  err := enc.Encode(saved_block)

  if err != nil {
    log.Fatal("encode:", err)
  }

  fmt.Println("SERIALIZED INTO BLOCK", filename, network.Len(), "BYTES", "( PER RECORD", network.Len() / len(records), ")");

  w, _ := os.Create(filename)
  network.WriteTo(w);


}

func (tb *TableBlock) ReadFromFile(fname string) {

}

