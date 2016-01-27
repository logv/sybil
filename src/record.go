package edb

type Record struct {
  Ints []IntField;
  Strs []StrField;
  Sets []SetField;
  Populated []bool;

  block *TableBlock;
}

func (r *Record) getStrVal(name string) (int, bool) {
  id := r.block.get_key_id(name);

  is := r.Strs[id]
  ok := r.Populated[id]
  return int(is), ok
}

func (r *Record) getIntVal(name string) (int, bool) {
  id := r.block.get_key_id(name);

  is := r.Ints[id]
  ok := r.Populated[id]
  return int(is), ok
}

func (r *Record) getVal(name string) (int, bool) {
  ret, ok := r.getStrVal(name);
  if !ok {
    ret, ok = r.getIntVal(name);
    if !ok {
      // TODO: throw error
      return 0, false
    }
  }

  return ret, true

}

func (r *Record) ResizeFields(length int16) {
  if int(length) >= len(r.Strs) {
    delta := int(length) - len(r.Strs) + 1
    delta_records := make([]StrField, delta*2)

    r.Strs = append(r.Strs, delta_records...)
  }

  if int(length) >= len(r.Populated) {
    delta := int(length) - len(r.Populated) + 1
    delta_records := make([]bool, delta*2)

    r.Populated = append(r.Populated, delta_records...)
  }

  if int(length) >= len(r.Ints) {
    delta := int(length) - len(r.Ints) + 1
    delta_records := make([]IntField, delta*2)

    r.Ints = append(r.Ints, delta_records...)
  }


}

func (r *Record) AddStrField(name string, val string) {
  name_id := r.block.get_key_id(name)

  col := r.block.getColumnInfo(name_id)
  value_id := col.get_val_id(val)

  r.ResizeFields(name_id)
  r.Strs[name_id] = StrField(value_id)
  r.Populated[name_id] = true
}

func (r *Record) AddIntField(name string, val int) {
  name_id := r.block.get_key_id(name)
  r.block.table.update_int_info(name_id, val)

  r.ResizeFields(name_id)
  r.Ints[name_id] = IntField(val)
  r.Populated[name_id] = true
}

func (r *Record) AddSetField(name string, val []string) {
  name_id := r.block.get_key_id(name)
  vals := make([]int32, len(val))
  for i, v := range(val) {
    col := r.block.getColumnInfo(name_id)
    vals[i] = col.get_val_id(v);
  }

  r.Sets[name_id] = SetField(vals)
}
