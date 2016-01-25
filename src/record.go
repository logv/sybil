package edb

type Record struct {
  Ints map[int16]IntField
  Strs map[int16]StrField
  Sets map[int16]SetField

  table *Table;
}

func (r *Record) getStrVal(name string) (int, bool) {
  id := r.table.get_key_id(name);

  is, ok := r.Strs[id]
  return int(is), ok
}

func (r *Record) getIntVal(name string) (int, bool) {
  id := r.table.get_key_id(name);

  is, ok := r.Ints[id]
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

func (r *Record) AddStrField(name string, val string) {
  name_id := r.table.get_key_id(name)
  value_id := r.table.get_val_id(val)
  r.Strs[name_id] = StrField(value_id)
}

func (r *Record) AddIntField(name string, val int) {
  name_id := r.table.get_key_id(name)
  r.table.update_int_info(name_id, val)
  r.Ints[name_id] = IntField(val)
}

func (r *Record) AddSetField(name string, val []string) {
  name_id := r.table.get_key_id(name)
  vals := make([]int32, len(val))
  for i, v := range(val) {
    vals[i] = r.table.get_val_id(v);
  }

  r.Sets[name_id] = SetField(vals)
}
