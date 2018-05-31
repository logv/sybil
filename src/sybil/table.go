package sybil

import "sync"
import "os"
import "path"

var ROW_STORE_BLOCK = "ROW_STORE"
var NULL_BLOCK = "NULL_BLOCK"

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

	stringIDMu *sync.RWMutex
	recordMu   *sync.Mutex
	blockMu    *sync.Mutex
}

var LOADED_TABLES = make(map[string]*Table)
var CHUNK_SIZE = 1024 * 8 * 8
var CHUNK_THRESHOLD = CHUNK_SIZE / 8

var tableMu sync.Mutex

// This is a singleton constructor for Tables
func GetTable(name string) *Table {
	tableMu.Lock()
	defer tableMu.Unlock()

	t, ok := LOADED_TABLES[name]
	if ok {
		return t
	}

	t = &Table{Name: name}
	LOADED_TABLES[name] = t

	t.initDataStructures()

	return t
}

// UnloadTable de-registers a table.
func UnloadTable(name string) {
	tableMu.Lock()
	delete(LOADED_TABLES, name)
	tableMu.Unlock()
}

func (t *Table) initDataStructures() {
	t.keyStringIDLookup = make(map[int16]string)
	t.valStringIDLookup = make(map[int32]string)

	t.KeyTable = make(map[string]int16)
	t.KeyTypes = make(map[int16]int8)

	t.BlockList = make(map[string]*TableBlock)

	t.BlockInfoCache = make(map[string]*SavedColumnInfo)
	t.NewBlockInfos = make([]string, 0)

	t.StrInfo = make(StrInfoTable)
	t.IntInfo = make(IntInfoTable)

	t.LastBlock = newTableBlock()
	t.LastBlock.RecordList = t.newRecords
	t.initLocks()
}

func (t *Table) initLocks() {
	t.stringIDMu = &sync.RWMutex{}
	t.recordMu = &sync.Mutex{}
	t.blockMu = &sync.Mutex{}

}

func (t *Table) getStringForKey(id int) string {
	return t.keyStringIDLookup[int16(id)]
}

func (t *Table) populateStringIDLookup() {
	t.stringIDMu.Lock()
	defer t.stringIDMu.Unlock()

	t.keyStringIDLookup = make(map[int16]string)
	t.valStringIDLookup = make(map[int32]string)

	for k, v := range t.KeyTable {
		t.keyStringIDLookup[v] = k
	}

	for _, b := range t.BlockList {
		if b.columns == nil && b.Name != ROW_STORE_BLOCK {
			Debug("WARNING, BLOCK", b.Name, "IS SUSPECT! REMOVING FROM BLOCKLIST")
			t.blockMu.Lock()
			delete(t.BlockList, b.Name)
			t.blockMu.Unlock()
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
	t.stringIDMu.RLock()
	id, ok := t.KeyTable[name]
	t.stringIDMu.RUnlock()
	if ok {
		return int16(id)
	}

	t.stringIDMu.Lock()
	defer t.stringIDMu.Unlock()
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

	t.recordMu.Lock()
	t.newRecords = append(t.newRecords, &r)
	t.recordMu.Unlock()
	return &r
}

func (t *Table) PrintRecord(r *Record) {
	Print("RECORD", r)

	for name, val := range r.Ints {
		if r.Populated[name] == INT_VAL {
			col := r.block.GetColumnInfo(int16(name))
			Print("  ", name, col.getStringForKey(name), val)
		}
	}
	for name, val := range r.Strs {
		if r.Populated[name] == STR_VAL {
			col := r.block.GetColumnInfo(int16(name))
			Print("  ", name, col.getStringForKey(name), col.getStringForVal(int32(val)))
		}
	}
	for name, vals := range r.SetMap {
		if r.Populated[name] == SET_VAL {
			col := r.block.GetColumnInfo(int16(name))
			for _, val := range vals {
				Print("  ", name, col.getStringForKey(int(name)), val, col.getStringForVal(int32(val)))

			}

		}
	}
}

func (t *Table) MakeDir(flags *FlagDefs) {
	tabledir := path.Join(*flags.DIR, t.Name)
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
