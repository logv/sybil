package edb

type IntArr map[int16]IntField;
type StrArr map[int16]StrField;
type SetArr map[int16]SetField;

type IntField int32
type StrField int32
type SetField []int32
