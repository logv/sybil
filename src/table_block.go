package edb

import "sync"

// Table Block should have a bunch of metadata next to it, too
type TableBlock struct {
  RecordList []*Record
  StringTable map[string]int32 // String Value lookup

  string_id_m *sync.Mutex
  val_string_id_lookup map[int32]string
  table *Table
}

func newTableBlock() TableBlock {

  tb := TableBlock{}
  tb.StringTable = make(map[string]int32)
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

func (tb *TableBlock) get_val_id(name string) int32 {

  id, ok := tb.StringTable[name]

  if ok {
    return int32(id);
  }


  tb.string_id_m.Lock();
  tb.StringTable[name] = int32(len(tb.StringTable));
  tb.val_string_id_lookup[tb.StringTable[name]] = name;
  tb.string_id_m.Unlock();
  return tb.StringTable[name];
}


func (tb *TableBlock) get_string_for_key(id int16) string {
  return tb.table.get_string_for_key(id)

}

func (tb *TableBlock) get_string_for_val(id int32) string {
  val, _ := tb.val_string_id_lookup[id];
  return val
}
