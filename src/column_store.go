package edb

type SavedIntColumn struct {
  Name int16
  Value int32
  Records []int32
}

type SavedSetColumn struct {
  Name int16
  Values []int32
  Records []int32
}

type SavedStrColumn struct {
  Name int16
  Value int32
  Records []int32

}

type SavedInts struct {
  Bins []SavedIntColumn
}
type SavedStrs struct {
  Bins []SavedStrColumn
  StringTable map[string]int32
}
type SavedSets struct {
  Bins []SavedSetColumn
}
