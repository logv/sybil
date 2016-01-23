package edb

type Record struct {
  Ints []IntField
  Strs []StrField
  Sets []SetField

  table *Table;
}

func (r *Record) getStrVal(name string) (int, bool) {
  id := r.table.get_string_id(name);

  for i := 0; i < len(r.Strs); i++ {
    if r.Strs[i].Name == id {
      return r.Strs[i].Value, true;
    }
  }

  return 0, false;
}

func (r *Record) getIntVal(name string) (int, bool) {
 
  id := r.table.get_string_id(name);

  for i := 0; i < len(r.Ints); i++ {
    if r.Ints[i].Name == id {
      return r.Ints[i].Value, true;
    }
  }

  return 0, false;
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
  name_id := r.table.get_string_id(name)
  value_id := r.table.get_string_id(val)
  r.Strs = append(r.Strs, StrField{name_id, value_id})
}

func (r *Record) AddIntField(name string, val int) {
  name_id := r.table.get_string_id(name)
  r.Ints = append(r.Ints, IntField{name_id, val})
}

func (r *Record) AddSetField(name string, val []string) {
  name_id := r.table.get_string_id(name)
  vals := make([]int, len(val))
  for i, v := range(val) {
    vals[i] = r.table.get_string_id(v);
  }

  r.Sets = append(r.Sets, SetField{name_id, vals})
}


  
