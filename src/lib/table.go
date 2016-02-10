package pcs

import "fmt"
import "log"
import "sync"

type Table struct {
	Name      string
	BlockList map[string]*TableBlock
	KeyTable  map[string]int16 // String Key Names
	KeyTypes  map[int16]int8

	// Need to keep track of the last block we've used, right?
	LastBlockId int
	LastBlock   TableBlock

	StrInfo StrInfoTable
	IntInfo IntInfoTable

	// List of new records that haven't been saved to file yet
	newRecords RecordList

	key_string_id_lookup map[int16]string
	val_string_id_lookup map[int32]string

	string_id_m *sync.RWMutex
	record_m    *sync.Mutex
	block_m     *sync.Mutex
}

var LOADED_TABLES = make(map[string]*Table)
var CHUNK_SIZE = 1024 * 64
var table_m sync.Mutex

// This is a singleton constructor for Tables
func GetTable(name string) *Table {
	table_m.Lock()
	defer table_m.Unlock()

	t, ok := LOADED_TABLES[name]
	if ok {
		return t
	}

	t = &Table{Name: name}
	LOADED_TABLES[name] = t
	t.key_string_id_lookup = make(map[int16]string)
	t.val_string_id_lookup = make(map[int32]string)

	t.KeyTable = make(map[string]int16)
	t.KeyTypes = make(map[int16]int8)

	t.BlockList = make(map[string]*TableBlock, 0)
	t.StrInfo = make(StrInfoTable)
	t.IntInfo = make(IntInfoTable)

	t.LastBlock = newTableBlock()
	t.LastBlock.RecordList = t.newRecords

	t.string_id_m = &sync.RWMutex{}
	t.record_m = &sync.Mutex{}
	t.block_m = &sync.Mutex{}

	return t
}

func (t *Table) get_string_for_key(id int) string {
	val, _ := t.key_string_id_lookup[int16(id)]
	return val
}

func (t *Table) populate_string_id_lookup() {
	t.string_id_m.Lock()
	defer t.string_id_m.Unlock()

	t.key_string_id_lookup = make(map[int16]string)
	t.val_string_id_lookup = make(map[int32]string)

	for k, v := range t.KeyTable {
		t.key_string_id_lookup[v] = k
	}

	for _, b := range t.BlockList {
		for _, c := range b.columns {
			for k, v := range c.StringTable {
				c.val_string_id_lookup[v] = k
			}
		}

	}
}

func (t *Table) get_key_id(name string) int16 {
	t.string_id_m.RLock()
	id, ok := t.KeyTable[name]
	t.string_id_m.RUnlock()
	if ok {
		return int16(id)
	}

	t.string_id_m.Lock()
	defer t.string_id_m.Unlock()
	existing, ok := t.KeyTable[name]
	if ok {
		return existing
	}

	t.KeyTable[name] = int16(len(t.KeyTable))
	t.key_string_id_lookup[t.KeyTable[name]] = name

	return int16(t.KeyTable[name])
}

func (t *Table) set_key_type(name_id int16, col_type int8) {
	cur_type, ok := t.KeyTypes[name_id]
	if !ok {
		t.KeyTypes[name_id] = col_type
	} else {
		if cur_type != col_type {
			log.Println("Warning, trying to over-write column type for key", t.get_string_for_key(int(name_id)))
		}
	}

}

func (t *Table) NewRecord() *Record {
	r := Record{Ints: IntArr{}, Strs: StrArr{}}

	b := t.LastBlock
	b.table = t
	r.block = &b

	t.record_m.Lock()
	t.newRecords = append(t.newRecords, &r)
	t.record_m.Unlock()
	return &r
}

func (t *Table) PrintRecord(r *Record) {
	log.Println("RECORD", r)

	for name, val := range r.Ints {
		if r.Populated[name] == INT_VAL {
			col := r.block.getColumnInfo(int16(name))
			fmt.Println("  ", name, col.get_string_for_key(name), val)
		}
	}
	for name, val := range r.Strs {
		if r.Populated[name] == STR_VAL {
			col := r.block.getColumnInfo(int16(name))
			fmt.Println("  ", name, col.get_string_for_key(name), col.get_string_for_val(int32(val)))
		}
	}
}

func (t *Table) PrintRecords(records RecordList) {
	for i := 0; i < len(records); i++ {
		t.PrintRecord(records[i])
	}
}
