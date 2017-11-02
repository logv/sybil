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

	StringIDMutex     *sync.Mutex
	ValStringIDLookup map[int32]string
}

func NewTableColumn(tb *TableBlock) *TableColumn {
	tc := TableColumn{}
	tc.StringTable = make(map[string]int32)
	tc.ValStringIDLookup = make(map[int32]string)
	tc.StringIDMutex = &sync.Mutex{}
	tc.RCache = make(map[int]bool)

	return &tc
}
