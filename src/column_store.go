package edb

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
}

type SavedInts struct {
  Name int16
  Bins []SavedIntColumn
}
type SavedStrs struct {
  Name int16
  Bins []SavedStrColumn
  StringTable map[string]int32
}
type SavedSets struct {
  Name int16
  Bins []SavedSetColumn
}
