package structs

type IntArr []IntField
type StrArr []StrField
type SetArr []SetField
type SetMap map[int16]SetField

type IntField int64
type StrField int32
type SetField []int32

type IntInfoTable map[int16]*IntInfo
type StrInfoTable map[int16]*StrInfo
