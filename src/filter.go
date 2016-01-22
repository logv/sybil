package edb

type Filter interface {
  Filter(Record) bool;
}

type NoFilter struct { }
func (f NoFilter) Filter(r Record) bool {
  return false;
}
