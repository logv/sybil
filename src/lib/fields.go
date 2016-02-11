package pcs

type IntArr []IntField
type StrArr []StrField
type SetArr []SetField
type SetMap map[int16]SetField

type IntField int64
type StrField int32
type SetField []int32
