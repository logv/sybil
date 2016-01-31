package edb

import "fmt"
import "bytes"
import "log"
import "os"
import "encoding/gob"
import "strings"
import "sort"

type ValueMap map[int32][]uint32
type SetMap map[int32][]int32

func delta_encode(same_map map[int16]ValueMap) {
  for _, col := range same_map {
    for _, records := range col {
      prev := uint32(0)
      for i, v := range records {
	records[int32(i)] = v - prev
	prev = v
	

      }
    }
  }

}

// this is used to record the buckets when building the column
// blobs
func record_value(same_map map[int16]ValueMap, index int32, name int16, value int32) {
  s, ok := same_map[name]
  if !ok {
    same_map[name] = ValueMap{}
    s = same_map[name]
  }

  vi := int32(value)

  s[vi] = append(s[vi], uint32(index))
}





func (tb *TableBlock) getColumnInfo(name_id int16) *TableColumn {
  col, ok := tb.columns[name_id]
  if !ok {
    col = tb.newTableColumn()
    tb.columns[name_id] = col
  }

  return col
}

func (tb *TableBlock) SaveIntsToColumns(dirname string, same_ints map[int16]ValueMap) {
  // now make the dir and shoot each blob out into a separate file

  // SAVED TO A SINGLE BLOCK ON DISK, NOW TO SAVE IT OUT TO SEPARATE VALUES
  os.MkdirAll(dirname, 0777)
  for k, v := range same_ints {
    col_name := tb.get_string_for_key(k) 
    if col_name == "" {
      fmt.Println("CANT FIGURE OUT FIELD NAME FOR", k, "SOMETHING IS PROBABLY AWRY")
      continue
    }
    intCol := SavedInts{}
    intCol.NameId = k
    intCol.Name = col_name
    intCol.DeltaEncodedIDs = DELTA_ENCODE_RECORD_IDS

    for bucket, records := range v {
      si := SavedIntColumn{Value: bucket, Records: records}
      intCol.Bins = append(intCol.Bins, si)

      // bookkeeping for info.db
      tb.update_int_info(k, int(bucket))
    }

    // Sort the int buckets before saving them, so we don't have to sort them after reading.
    sort.Sort(SortIntsByVal(intCol.Bins))


    col_fname := fmt.Sprintf("%s/int_%s.db", dirname, tb.get_string_for_key(k))

    var network bytes.Buffer // Stand-in for the network.

    // Create an encoder and send a value.
    enc := gob.NewEncoder(&network)
    err := enc.Encode(intCol)

    if err != nil {
      log.Fatal("encode:", err)
    }

    fmt.Println("SERIALIZED COLUMN BLOCK", col_fname, network.Len(), "BYTES", "( PER RECORD", network.Len() / len(tb.RecordList), ")");

    w, _ := os.Create(col_fname)
    network.WriteTo(w);
  }




}

func (tb *TableBlock) SaveStrsToColumns(dirname string, same_strs map[int16]ValueMap) {
  for k, v := range same_strs {
    col_name := tb.get_string_for_key(k) 
    if col_name == "" {
      // TODO: validate what this means. I think it means reading 'null' values off disk
      // when pulling off incomplete records
      fmt.Println("CANT FIGURE OUT FIELD NAME FOR", k, "PROBABLY AN ERRONEOUS FIELD")
      continue
    }
    strCol := SavedStrs{}
    strCol.Name = col_name
    strCol.NameId = k
    strCol.DeltaEncodedIDs = DELTA_ENCODE_RECORD_IDS
    temp_block := newTableBlock()

    temp_col := temp_block.getColumnInfo(k)
    tb_col := tb.getColumnInfo(k)
    for bucket, records := range v {

      // migrating string definitions from column definitions
      str_id := temp_col.get_val_id(tb_col.get_string_for_val(bucket))

      si := SavedStrColumn{Value: str_id, Records: records}
      strCol.Bins = append(strCol.Bins, si)

      // also bookkeeping to be used later inside the block info.db, IMO
      tb.update_str_info(k, int(bucket), len(records))
    }

    tb.get_str_info(k).prune()

    strCol.StringTable = make([]string, len(temp_col.StringTable))
    for str, id := range temp_col.StringTable {
      strCol.StringTable[id] = str
    }

    col_fname := fmt.Sprintf("%s/str_%s.db", dirname, tb.get_string_for_key(k))

    var network bytes.Buffer // Stand-in for the network.

    // Create an encoder and send a value.
    enc := gob.NewEncoder(&network)
    err := enc.Encode(strCol)

    if err != nil {
      log.Fatal("encode:", err)
    }

    fmt.Println("SERIALIZED COLUMN BLOCK", col_fname, network.Len(), "BYTES", "( PER RECORD", network.Len() / len(tb.RecordList), ")");

    w, _ := os.Create(col_fname)
    network.WriteTo(w);

  }
}

func (tb *TableBlock) SaveInfoToColumns(dirname string) {
  records := tb.RecordList

  // Now to save block info...
  col_fname := fmt.Sprintf("%s/info.db", dirname)

  var network bytes.Buffer // Stand-in for the network.

  // Create an encoder and send a value.
  enc := gob.NewEncoder(&network)
  colInfo := SavedColumnInfo{NumRecords: int32(len(records)), IntInfo: INT_INFO_BLOCK[tb.Name], StrInfo: STR_INFO_BLOCK[tb.Name] }
  err := enc.Encode(colInfo)

  if err != nil {
    log.Fatal("encode:", err)
  }

  fmt.Println("SERIALIZED BLOCK INFO", col_fname, network.Len(), "BYTES", "( PER RECORD", network.Len() / len(records), ")");

  w, _ := os.Create(col_fname)
  network.WriteTo(w);
}


type SeparatedColumns struct {
  ints map[int16]ValueMap
  strs map[int16]ValueMap
  sets map[int16]SetMap
}

func (tb *TableBlock) SeparateRecordsIntoColumns() SeparatedColumns {
  records := tb.RecordList

  // making a cross section of records that share values
  // goes from fieldname{} -> value{} -> record
  same_ints := make(map[int16]ValueMap)
  same_strs := make(map[int16]ValueMap)
  same_sets := make(map[int16]SetMap)

  // parse record list and transfer book keeping data into the current
  // table block, as well as separate record values by column type
  for i, r := range records {
    for k, v := range r.Ints {
      if r.Populated[k] == INT_VAL {
	record_value(same_ints, int32(i), int16(k), int32(v))
      }
    }
    for k, v := range r.Strs {
      // transition key from the 
      col := r.block.getColumnInfo(int16(k))
      new_col := tb.getColumnInfo(int16(k))

      v_name := col.get_string_for_val(int32(v))
      v_id := new_col.get_val_id(v_name)

      // record the transitioned key
      if r.Populated[k] == STR_VAL {
	record_value(same_strs, int32(i), int16(k), int32(v_id))
      }
    }
    for k, v := range r.Sets {
      s, ok := same_sets[int16(k)]
      if !ok {
        s = SetMap{}
        same_sets[int16(k)] = s
      }
      s[int32(i)] = v
    }
  }

  if DELTA_ENCODE_RECORD_IDS {
    delta_encode(same_ints)
    delta_encode(same_strs)
  }

  ret := SeparatedColumns{ ints: same_ints, strs: same_strs, sets: same_sets }
  return ret

}

func (tb *TableBlock) SaveToColumns(filename string) {
  dirname := strings.Replace(filename, ".db", "", 1)

  // Important to set the BLOCK's dirName so we can keep track 
  // of the various block infos
  tb.Name = dirname

  partialname := fmt.Sprintf("%s.partial", dirname )
  os.RemoveAll(partialname)
  oldblock := fmt.Sprintf("%s.old", dirname)

  separated_columns := tb.SeparateRecordsIntoColumns()

  tb.SaveIntsToColumns(partialname, separated_columns.ints)
  tb.SaveStrsToColumns(partialname, separated_columns.strs)
  tb.SaveInfoToColumns(partialname)

  fmt.Println("FINISHED BLOCK", partialname, "RELINKING TO", dirname)

  // remove the old block
  os.RemoveAll(oldblock)
  err := os.Rename(dirname, oldblock)
  err = os.Rename(partialname, dirname)

  if err == nil {
    os.RemoveAll(oldblock)
  } else {
    fmt.Println("ERROR SAVING BLOCK", err)
  }
}




