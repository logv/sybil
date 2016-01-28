package edb

// ByAge implements sort.Interface for []Person based on
// the Age field.
type SortIntsByVal []SavedIntColumn

func (a SortIntsByVal) Len() int           { return len(a) }
func (a SortIntsByVal) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortIntsByVal) Less(i, j int) bool { return a[i].Value < a[j].Value }



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
  Name int16
  Bins []SavedIntColumn
}
type SavedStrs struct {
  Name int16
  Bins []SavedStrColumn
  StringTable []string
}
type SavedSets struct {
  Name int16
  Bins []SavedSetColumn
}
