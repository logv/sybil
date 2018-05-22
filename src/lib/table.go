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

	keyStringIdLookup map[int16]string
	valStringIdLookup map[int32]string

	// This is used for join tables
	joinLookup map[string]*Record

	stringIdM *sync.RWMutex
	recordM   *sync.Mutex
	blockM    *sync.Mutex
}

var LOADED_TABLES = make(map[string]*Table)
var CHUNK_SIZE = 1024 * 8 * 8
var CHUNK_THRESHOLD = CHUNK_SIZE / 8

var tableM sync.Mutex

// This is a singleton constructor for Tables
func GetTable(name string) *Table {
	tableM.Lock()
	defer tableM.Unlock()

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
	tableM.Lock()
	delete(LOADED_TABLES, name)
	tableM.Unlock()
}

func (t *Table) initDataStructures() {
	t.keyStringIdLookup = make(map[int16]string)
	t.valStringIdLookup = make(map[int32]string)

	t.KeyTable = make(map[string]int16)
	t.KeyTypes = make(map[int16]int8)

	t.BlockList = make(map[string]*TableBlock, 0)

	t.BlockInfoCache = make(map[string]*SavedColumnInfo, 0)
	t.NewBlockInfos = make([]string, 0)

	t.StrInfo = make(StrInfoTable)
	t.IntInfo = make(IntInfoTable)

	t.LastBlock = newTableBlock()
	t.LastBlock.RecordList = t.newRecords
	t.initLocks()
}

func (t *Table) initLocks() {
	t.stringIdM = &sync.RWMutex{}
	t.recordM = &sync.Mutex{}
	t.blockM = &sync.Mutex{}

}

func (t *Table) getStringForKey(id int) string {
	val, _ := t.keyStringIdLookup[int16(id)]
	return val
}

func (t *Table) populateStringIdLookup() {
	t.stringIdM.Lock()
	defer t.stringIdM.Unlock()

	t.keyStringIdLookup = make(map[int16]string)
	t.valStringIdLookup = make(map[int32]string)

	for k, v := range t.KeyTable {
		t.keyStringIdLookup[v] = k
	}

	for _, b := range t.BlockList {
		if b.columns == nil && b.Name != ROW_STORE_BLOCK {
			Debug("WARNING, BLOCK", b.Name, "IS SUSPECT! REMOVING FROM BLOCKLIST")
			t.blockM.Lock()
			delete(t.BlockList, b.Name)
			t.blockM.Unlock()
			continue
		}
		for _, c := range b.columns {
			for k, v := range c.StringTable {
				c.valStringIdLookup[v] = k
			}
		}

	}
}

func (t *Table) getKeyId(name string) int16 {
	t.stringIdM.RLock()
	id, ok := t.KeyTable[name]
	t.stringIdM.RUnlock()
	if ok {
		return int16(id)
	}

	t.stringIdM.Lock()
	defer t.stringIdM.Unlock()
	existing, ok := t.KeyTable[name]
	if ok {
		return existing
	}

	t.KeyTable[name] = int16(len(t.KeyTable))
	t.keyStringIdLookup[t.KeyTable[name]] = name

	return int16(t.KeyTable[name])
}

func (t *Table) setKeyType(nameId int16, colType int8) bool {
	curType, ok := t.KeyTypes[nameId]
	if !ok {
		t.KeyTypes[nameId] = colType
	} else {
		if curType != colType {
			Debug("TABLE", t.KeyTable)
			Debug("TYPES", t.KeyTypes)
			Warn("trying to over-write column type for key ", nameId, t.getStringForKey(int(nameId)), " OLD TYPE", curType, " NEW TYPE", colType)
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

	t.recordM.Lock()
	t.newRecords = append(t.newRecords, &r)
	t.recordM.Unlock()
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

func (t *Table) MakeDir() {
	tabledir := path.Join(*FLAGS.DIR, t.Name)
	os.MkdirAll(tabledir, 0755)
}

func (t *Table) PrintRecords(records RecordList) {
	for i := 0; i < len(records); i++ {
		t.PrintRecord(records[i])
	}
}

func (t *Table) GetColumnType(v string) int8 {
	colId := t.getKeyId(v)
	return t.KeyTypes[colId]
}
