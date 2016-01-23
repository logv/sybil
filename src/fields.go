package edb

type IntArr []IntField;
type StrArr []StrField;
type SetArr []SetField;

type IntField struct {
  Name int;
  Value int;
}

// value is held in the look up table!
type StrField struct {
  Name int;
  Value int;
}

// a set field holds sets of strings
type SetField struct {
  Name int;
  Value []int;
}

