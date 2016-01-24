package edb

type IntArr map[int]IntField;
type StrArr map[int]StrField;
type SetArr map[int]SetField;

type IntField int32
type StrField int32
type SetField []int
