package sybil

import "sync"
import "os"
import "path"

var RowStoreBlock = "RowStore"

type Table struct {
	Name      string
	BlockList map[string]*TableBlock
	KeyTable  map[string]int16 // String Key Names
	KeyTypes  map[int16]int8

	// Need to keep track of the last block we've used, right?
	LastBlock TableBlock
	RowBlock  *TableBlock

	StrInfo StrInfoTable
	IntInfo IntInfoTable

	BlockInfoCache map[string]*SavedColumnInfo
	NewBlockInfos  []string

	// List of new records that haven't been saved to file yet
	newRecords RecordList

	keyStringIDLookup map[int16]string
	valStringIDLookup map[int32]string

	// This is used for join tables
	joinLookup map[string]*Record

	stringIDMutex *sync.RWMutex
	recordMutex       *sync.Mutex
	blockMutex        *sync.Mutex
}

var LoadedTables = make(map[string]*Table)
var ChunkSize = 1024 * 8 * 8
var ChunkThreshold = ChunkSize / 8

var tableM sync.Mutex

// This is a singleton constructor for Tables
func GetTable(name string) *Table {
	tableM.Lock()
	defer tableM.Unlock()

	t, ok := LoadedTables[name]
	if ok {
		return t
	}

	t = &Table{Name: name}
	LoadedTables[name] = t

	t.initDataStructures()

	return t
}

func (t *Table) initDataStructures() {
	t.keyStringIDLookup = make(map[int16]string)
	t.valStringIDLookup = make(map[int32]string)

	t.KeyTable = make(map[string]int16)
	t.KeyTypes = make(map[int16]int8)

	t.BlockList = make(map[string]*TableBlock, 0)

	t.BlockInfoCache = make(map[string]*SavedColumnInfo, 0)
	t.NewBlockInfos = make([]string, 0)

	t.StrInfo = make(StrInfoTable)
	t.IntInfo = make(IntInfoTable)

	t.LastBlock = newTableBlock()
	t.LastBlock.RecordList = t.newRecords

	t.stringIDMutex = &sync.RWMutex{}
	t.recordMutex = &sync.Mutex{}
	t.blockMutex = &sync.Mutex{}

}

func (t *Table) getStringForKey(id int) string {
	val, _ := t.keyStringIDLookup[int16(id)]
	return val
}

func (t *Table) populateStringIDLookup() {
	t.stringIDMutex.Lock()
	defer t.stringIDMutex.Unlock()

	t.keyStringIDLookup = make(map[int16]string)
	t.valStringIDLookup = make(map[int32]string)

	for k, v := range t.KeyTable {
		t.keyStringIDLookup[v] = k
	}

	for _, b := range t.BlockList {
		if b.columns == nil && b.Name != RowStoreBlock {
			Debug("WARNING, BLOCK", b.Name, "IS SUSPECT! REMOVING FROM BLOCKLIST")
			t.blockMutex.Lock()
			delete(t.BlockList, b.Name)
			t.blockMutex.Unlock()
			continue
		}
		for _, c := range b.columns {
			for k, v := range c.StringTable {
				c.valStringIDLookup[v] = k
			}
		}

	}
}

func (t *Table) getKeyID(name string) int16 {
	t.stringIDMutex.RLock()
	id, ok := t.KeyTable[name]
	t.stringIDMutex.RUnlock()
	if ok {
		return int16(id)
	}

	t.stringIDMutex.Lock()
	defer t.stringIDMutex.Unlock()
	existing, ok := t.KeyTable[name]
	if ok {
		return existing
	}

	t.KeyTable[name] = int16(len(t.KeyTable))
	t.keyStringIDLookup[t.KeyTable[name]] = name

	return int16(t.KeyTable[name])
}

func (t *Table) setKeyType(nameID int16, colType int8) bool {
	curType, ok := t.KeyTypes[nameID]
	if !ok {
		t.KeyTypes[nameID] = colType
	} else {
		if curType != colType {
			Debug("TABLE", t.KeyTable)
			Debug("TYPES", t.KeyTypes)
			Warn("trying to over-write column type for key ", nameID, t.getStringForKey(int(nameID)), " OLD TYPE", curType, " NEW TYPE", colType)
			return false
		}
	}

	return true

}

func (t *Table) NewRecord() *Record {
	r := Record{Ints: IntArr{}, Strs: StrArr{}}

	b := t.LastBlock
	b.table = t
	r.block = &b

	t.recordMutex.Lock()
	t.newRecords = append(t.newRecords, &r)
	t.recordMutex.Unlock()
	return &r
}

func (t *Table) PrintRecord(r *Record) {
	Print("RECORD", r)

	for name, val := range r.Ints {
		if r.Populated[name] == IntVal {
			col := r.block.GetColumnInfo(int16(name))
			Print("  ", name, col.getStringForKey(name), val)
		}
	}
	for name, val := range r.Strs {
		if r.Populated[name] == StrVal {
			col := r.block.GetColumnInfo(int16(name))
			Print("  ", name, col.getStringForKey(name), col.getStringForVal(int32(val)))
		}
	}
	for name, vals := range r.SetMap {
		if r.Populated[name] == SetVal {
			col := r.block.GetColumnInfo(int16(name))
			for _, val := range vals {
				Print("  ", name, col.getStringForKey(int(name)), val, col.getStringForVal(int32(val)))

			}

		}
	}
}

func (t *Table) MakeDir() {
	tabledir := path.Join(*FLAGS.Dir, t.Name)
	os.MkdirAll(tabledir, 0755)
}

func (t *Table) PrintRecords(records RecordList) {
	for i := 0; i < len(records); i++ {
		t.PrintRecord(records[i])
	}
}

func (t *Table) GetColumnType(v string) int8 {
	colID := t.getKeyID(v)
	return t.KeyTypes[colID]
}
