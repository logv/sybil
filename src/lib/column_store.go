package edb

// ByAge implements sort.Interface for []Person based on
// the Age field.
type SortIntsByVal []SavedIntColumn

func (a SortIntsByVal) Len() int           { return len(a) }
func (a SortIntsByVal) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortIntsByVal) Less(i, j int) bool { return a[i].Value < a[j].Value }


// Before we save the new record list in a table, we tend to sort by time
type SortRecordsByTime []*Record
func (a SortRecordsByTime) Len() int           { return len(a) }
func (a SortRecordsByTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortRecordsByTime) Less(i, j int) bool { 
  t1, _ := a[i].getIntVal("time") 
  t2, _ := a[j].getIntVal("time") 

  return t1 < t2
}


type SavedIntColumn struct {
  Value int32
  Records []int32
}

type SavedSetColumn struct {
  Values []int32
  Records []int32
}

type SavedStrColumn struct {
  Value int32
  Records []int32

}

type SavedColumnInfo struct {
  NumRecords int32
  StrInfo StrInfoTable
  IntInfo IntInfoTable
}

type SavedInts struct {
  Name string
  NameId int16
  Bins []SavedIntColumn
}
type SavedStrs struct {
  Name string
  NameId int16
  Bins []SavedStrColumn
  StringTable []string
}
type SavedSets struct {
  Name string
  NameId int16
  Bins []SavedSetColumn
}
