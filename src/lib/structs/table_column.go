package structs

import "sync"

type SavedIntInfo map[string]*IntInfo
type SavedStrInfo map[string]*StrInfo

type SavedColumnInfo struct {
	NumRecords int32

	StrInfoMap SavedStrInfo
	IntInfoMap SavedIntInfo
}

type TableColumn struct {
	Type        int8
	StringTable map[string]int32
	RCache      map[int]bool

	Block *TableBlock

	StringIDMutex     *sync.Mutex
	ValStringIDLookup map[int32]string
}
