package sybil

import "sync"
import "os"
import "path"

var ROW_STORE_BLOCK = "ROW_STORE"

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

	key_string_id_lookup map[int16]string
	val_string_id_lookup map[int32]string

	// This is used for join tables
	join_lookup map[string]*Record

	string_id_m *sync.RWMutex
	record_m    *sync.Mutex
	block_m     *sync.Mutex
}

var LOADED_TABLES = make(map[string]*Table)
var CHUNK_SIZE = 1024 * 8 * 8
var CHUNK_THRESHOLD = CHUNK_SIZE / 8

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

	t.init_data_structures()

	return t
}

func (t *Table) init_data_structures() {
	t.key_string_id_lookup = make(map[int16]string)
	t.val_string_id_lookup = make(map[int32]string)

	t.KeyTable = make(map[string]int16)
	t.KeyTypes = make(map[int16]int8)

	t.BlockList = make(map[string]*TableBlock, 0)

	t.BlockInfoCache = make(map[string]*SavedColumnInfo, 0)
	t.NewBlockInfos = make([]string, 0)

	t.StrInfo = make(StrInfoTable)
	t.IntInfo = make(IntInfoTable)

	t.LastBlock = newTableBlock()
	t.LastBlock.RecordList = t.newRecords

	t.string_id_m = &sync.RWMutex{}
	t.record_m = &sync.Mutex{}
	t.block_m = &sync.Mutex{}

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
		if b.columns == nil && b.Name != ROW_STORE_BLOCK {
			Debug("WARNING, BLOCK", b.Name, "IS SUSPECT! REMOVING FROM BLOCKLIST")
			t.block_m.Lock()
			delete(t.BlockList, b.Name)
			t.block_m.Unlock()
			continue
		}
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

func (t *Table) set_key_type(name_id int16, col_type int8) bool {
	cur_type, ok := t.KeyTypes[name_id]
	if !ok {
		t.KeyTypes[name_id] = col_type
	} else {
		if cur_type != col_type {
			Debug("TABLE", t.KeyTable)
			Debug("TYPES", t.KeyTypes)
			Warn("trying to over-write column type for key ", name_id, t.get_string_for_key(int(name_id)), " OLD TYPE", cur_type, " NEW TYPE", col_type)
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

	t.record_m.Lock()
	t.newRecords = append(t.newRecords, &r)
	t.record_m.Unlock()
	return &r
}

func (t *Table) PrintRecord(r *Record) {
	Print("RECORD", r)

	for name, val := range r.Ints {
		if r.Populated[name] == INT_VAL {
			col := r.block.GetColumnInfo(int16(name))
			Print("  ", name, col.get_string_for_key(name), val)
		}
	}
	for name, val := range r.Strs {
		if r.Populated[name] == STR_VAL {
			col := r.block.GetColumnInfo(int16(name))
			Print("  ", name, col.get_string_for_key(name), col.get_string_for_val(int32(val)))
		}
	}
	for name, vals := range r.SetMap {
		if r.Populated[name] == SET_VAL {
			col := r.block.GetColumnInfo(int16(name))
			for _, val := range vals {
				Print("  ", name, col.get_string_for_key(int(name)), val, col.get_string_for_val(int32(val)))

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
	col_id := t.get_key_id(v)
	return t.KeyTypes[col_id]
}
