package edb

import "fmt"

type StrInfo struct {

}

type IntInfo struct {
  Min int
  Max int
  Avg float64
  Count int
}

var INT_INFO_TABLE = make(map[string]map[int16]*IntInfo)
var INT_INFO_BLOCK = make(map[string]map[int16]*IntInfo)

func update_str_info(int_info_table map[int16]*IntInfo, name int16, val int) {

}

func update_int_info(int_info_table map[int16]*IntInfo, name int16, val int) {
  info, ok := int_info_table[name]
  if !ok {
    info = &IntInfo{}
    int_info_table[name] = info
    info.Max = val
    info.Min = val
    info.Avg = float64(val)
    info.Count = 1
  }

  if info.Count > 1024 {
    return
  }

  if info.Max < val {
    info.Max = val
  }

  if info.Min > val {
    info.Min = val
  }
  
  info.Avg = info.Avg + (float64(val) - info.Avg) / float64(info.Count)

  info.Count++
}

func (t *Table) update_int_info(name int16, val int) {
  int_info_table, ok := INT_INFO_TABLE[t.Name]
  if !ok {
    int_info_table = make(map[int16]*IntInfo)
    INT_INFO_TABLE[t.Name] = int_info_table
  }


  update_int_info(int_info_table, name, val)
}

func (tb *TableBlock) update_int_info(name int16, val int) {
  int_info_table, ok := INT_INFO_BLOCK[tb.Name]
  if !ok {
    int_info_table = make(map[int16]*IntInfo)
    INT_INFO_TABLE[tb.Name] = int_info_table
  }


  update_int_info(int_info_table, name, val)
}

func (t *Table) get_int_info(name int16) *IntInfo {
  return INT_INFO_TABLE[t.Name][name]

}

func (tb *TableBlock) get_int_info(name int16) *IntInfo {
  return INT_INFO_BLOCK[tb.Name][name]

}

func (t *Table) PrintColInfo() {
  for k, v := range INT_INFO_TABLE[t.Name] {
    fmt.Println(k, v)
  }

}

