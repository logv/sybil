package edb

import "sync"

type TableColumn struct {
  Type string
  StringTable map[string]int32

  block *TableBlock

  string_id_m *sync.Mutex;
  val_string_id_lookup map[int32]string
}

func (tb *TableBlock) newTableColumn() *TableColumn {
  tc := TableColumn{}
  tc.StringTable = make(map[string]int32)
  tc.val_string_id_lookup = make(map[int32]string)
  tc.string_id_m = &sync.Mutex{}
  tc.block = tb

  return &tc
}

func (tc *TableColumn) get_val_id(name string) int32 {

  id, ok := tc.StringTable[name]

  if ok {
    return int32(id);
  }


  tc.string_id_m.Lock();
  tc.StringTable[name] = int32(len(tc.StringTable));
  tc.val_string_id_lookup[tc.StringTable[name]] = name;
  tc.string_id_m.Unlock();
  return tc.StringTable[name];
}


func (tc *TableColumn) get_string_for_val(id int32) string {
  val, _ := tc.val_string_id_lookup[id];
  return val
}

func (tc *TableColumn) get_string_for_key(id int) string {
  return tc.block.get_string_for_key(int16(id));
}


