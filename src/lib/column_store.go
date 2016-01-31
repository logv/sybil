package edb

// ByAge implements sort.Interface for []Person based on
// the Age field.
type SortIntsByVal []SavedIntColumn

func (a SortIntsByVal) Len() int           { return len(a) }
func (a SortIntsByVal) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortIntsByVal) Less(i, j int) bool { return a[i].Value < a[j].Value }


// Before we save the new record list in a table, we tend to sort by time
type RecordList []*Record
type SortRecordsByTime struct {
  RecordList

  ColId int16
}

func (a RecordList) Len() int           { return len(a) }
func (a RecordList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortRecordsByTime) Less(i, j int) bool { 
  t1 := a.RecordList[i].Ints[a.ColId]
  t2 := a.RecordList[j].Ints[a.ColId]

  return t1 < t2
}


type SavedIntColumn struct {
  Value int32
  Records []uint32
}

type SavedSetColumn struct {
  Values []int32
  Records []uint32
}

type SavedStrColumn struct {
  Value int32
  Records []uint32

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
