package edb

type Filter interface {
  Filter(*Record) bool;
}

type NoFilter struct { }
func (f NoFilter) Filter(r *Record) bool {
  return false;
}

type IntFilter struct {
  Field string
  FieldId int
  Op string
  Value int

  table *Table
}

func (filter IntFilter) Filter(r *Record) bool {
  for i := 0; i < len(r.Ints); i++ {
    field := r.Ints[i]
    if field.Name == filter.FieldId {
      switch filter.Op {
        case "gt":
          return field.Value < filter.Value

        case "lt":
          return field.Value > filter.Value

        default:

      }
    }
  }

  return false
}


func (t *Table) IntFilter(name string, op string, value int) IntFilter {
  intFilter := IntFilter{Field: name, FieldId: t.get_string_id(name), Op: op, Value: value}
  intFilter.table = t;

  return intFilter
  
}
