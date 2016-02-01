package edb

type Record struct {
  Ints []IntField;
  Strs []StrField;
  Sets []SetField;
  Populated []int;

  block *TableBlock;
}

var INT_VAL = 1;
var STR_VAL = 2;
var SET_VAL = 3;

func (r *Record) getStrVal(name string) (int, bool) {
  id := r.block.get_key_id(name);

  is := r.Strs[id]
  ok := r.Populated[id] == STR_VAL
  return int(is), ok
}

func (r *Record) getIntVal(name string) (int, bool) {
  id := r.block.get_key_id(name);

  is := r.Ints[id]
  ok := r.Populated[id] == INT_VAL
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
  // dont get fooled by zeroes
  if length <= 1 {
    length = 5
  }

  if int(length) >= len(r.Strs) {
    delta_records := make([]StrField, int(float64(length)*1.5))

    r.Strs = append(r.Strs, delta_records...)
  }

  if int(length) >= len(r.Populated) {
    delta_records := make([]int, int(float64(length)*1.5))

    r.Populated = append(r.Populated, delta_records...)
  }

  if int(length) >= len(r.Ints) {
    delta_records := make([]IntField, int(float64(length)*1.5))

    r.Ints = append(r.Ints, delta_records...)
  }
}

func (r *Record) AddStrField(name string, val string) {
  name_id := r.block.get_key_id(name)

  col := r.block.getColumnInfo(name_id)
  value_id := col.get_val_id(val)

  r.ResizeFields(name_id)
  r.Strs[name_id] = StrField(value_id)
  r.Populated[name_id] = STR_VAL
}

func (r *Record) AddIntField(name string, val int) {
  name_id := r.block.get_key_id(name)
  r.block.table.update_int_info(name_id, val)

  r.ResizeFields(name_id)
  r.Ints[name_id] = IntField(val)
  r.Populated[name_id] = INT_VAL
}

func (r *Record) AddSetField(name string, val []string) {
  name_id := r.block.get_key_id(name)
  vals := make([]int32, len(val))
  for i, v := range(val) {
    col := r.block.getColumnInfo(name_id)
    vals[i] = col.get_val_id(v);
  }

  r.ResizeFields(name_id)
  r.Sets[name_id] = SetField(vals)
}
