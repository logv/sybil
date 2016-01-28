package edb

import "fmt"
import "sort"

type StrInfo struct {
  TopStringCount map[int32]int
  Cardinality int
}

type IntInfo struct {
  Min int
  Max int
  Avg float64
  Count int
}

type IntInfoTable map[int16]*IntInfo
type StrInfoTable map[int16]*StrInfo

var TOP_STRING_COUNT = 1000
var INT_INFO_TABLE = make(map[string]IntInfoTable)
var INT_INFO_BLOCK = make(map[string]IntInfoTable)

var STR_INFO_TABLE = make(map[string]StrInfoTable)
var STR_INFO_BLOCK = make(map[string]StrInfoTable)


type StrInfoCol struct {
  Name int32
  Value int
}

type SortStrsByCount []StrInfoCol
func (a SortStrsByCount) Len() int           { return len(a) }
func (a SortStrsByCount) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortStrsByCount) Less(i, j int) bool { return a[i].Value < a[j].Value }

func (si *StrInfo) prune() {
  si.Cardinality = len(si.TopStringCount)

  if si.Cardinality > TOP_STRING_COUNT {
    interim := make([]StrInfoCol, len(si.TopStringCount))

    for s, c := range si.TopStringCount {
      interim[s] = StrInfoCol{Name: s, Value: c}
    }

    sort.Sort(SortStrsByCount(interim))

    for _, x := range interim[:len(si.TopStringCount) - TOP_STRING_COUNT - 1] {
      delete(si.TopStringCount, x.Name)
    }
  }

  fmt.Println("PRUNED ARR TO", len(si.TopStringCount), "FROM", si.Cardinality)
}

func update_str_info(str_info_table map[int16]*StrInfo, name int16, val int) {
  info, ok := str_info_table[name]
  if !ok {
    info = &StrInfo{}
    info.TopStringCount = make(map[int32]int)
    str_info_table[name] = info
  }

  info.TopStringCount[int32(val)] += 1
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

func (tb *TableBlock) update_str_info(name int16, val int) {
  str_info_table, ok := STR_INFO_BLOCK[tb.Name]
  if !ok {
    str_info_table = make(map[int16]*StrInfo)
    STR_INFO_BLOCK[tb.Name] = str_info_table
  }

  update_str_info(str_info_table, name, val)
}

func (tb *TableBlock) update_int_info(name int16, val int) {
  int_info_table, ok := INT_INFO_BLOCK[tb.Name]
  if !ok {
    int_info_table = make(map[int16]*IntInfo)
    INT_INFO_BLOCK[tb.Name] = int_info_table
  }

  update_int_info(int_info_table, name, val)
}

func (t *Table) get_int_info(name int16) *IntInfo {
  return INT_INFO_TABLE[t.Name][name]

}

func (tb *TableBlock) get_int_info(name int16) *IntInfo {
  return INT_INFO_BLOCK[tb.Name][name]
}

func (tb *TableBlock) get_str_info(name int16) *StrInfo {
  return STR_INFO_BLOCK[tb.Name][name]
}


func (t *Table) PrintColInfo() {
  for k, v := range INT_INFO_TABLE[t.Name] {
    fmt.Println(k, v)
  }

}

