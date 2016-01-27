package edb

type SavedInt struct {
  Name int16;
  Value int;
}

type SavedStr struct {
  Name int16;
  Value int;

}

type SavedSet struct {
  Name int16;
  Value []int32;
}

type SavedRecord struct {
  Ints []SavedInt
  Strs []SavedStr
  Sets []SavedSet
}


func (s SavedRecord) toRecord(tb *TableBlock) *Record {
  r := Record{}
  r.Ints = make(map[int16]IntField)
  r.Strs = make(map[int16]StrField)
  r.Sets = make(map[int16]SetField)

  for _, v := range s.Ints {
    r.Ints[v.Name] = IntField(v.Value)
    tb.table.update_int_info(v.Name, v.Value)
  }

  for _, v := range s.Strs {
    r.Strs[v.Name] = StrField(v.Value)
  }

  for _, v := range s.Sets {
    r.Sets[v.Name] = v.Value
  }

  r.block = tb;


  return &r
}

func (r Record) toSavedRecord(tb *TableBlock) *SavedRecord {
  s := SavedRecord{}
  for k, v := range r.Ints {
    s.Ints = append(s.Ints, SavedInt{k, int(v)})
  }

  for k, v := range r.Strs {
    col := r.block.getColumnInfo(k)
    v_name := col.get_string_for_val(int32(v))
    v_id := col.get_val_id(v_name)

    s.Strs = append(s.Strs, SavedStr{k, int(v_id)})
  }

  for k, v := range r.Sets {
    s.Sets = append(s.Sets, SavedSet{k, v})
  }

  return &s;

}
