package edb

import "fmt"

type Record struct {
  ints []IntField
  strs []StrField
  sets []SetField

  session_id int
  timestamp  int
}

var RECORD_LIST = make([]Record, 0)

// TODO: insert timestamp (or verify it exists)
// TODO: also verify the session_id exists
func NewRecord(ints IntArr, strs StrArr, sets SetArr) Record {
  r := Record{sets: sets, ints: ints, strs: strs}
  RECORD_LIST = append(RECORD_LIST, r)
  return r
}

func PrintRecords() {
  for i := 0; i < len(RECORD_LIST); i++ {
    fmt.Println("RECORD", RECORD_LIST[i])
  }

}
