package structs

import "sync"
import "runtime/debug"
import (
	"io/ioutil"
	"os"
	"path"

	. "github.com/logv/sybil/src/lib/config"
)

var ROW_STORE_BLOCK = "ROW_STORE"

type StrInfo struct {
	TopStringCount map[int32]int
	Cardinality    int
}

type IntInfo struct {
	Min   int64
	Max   int64
	Avg   float64
	M2    float64 // used for calculating std dev, expressed as M2 / (Count - 1)
	Count int
}

// This metadata is saved into <table>/info.db
type SavedTableInfo struct {
	Name     string
	KeyTable map[string]int16 // String Key Names
	KeyTypes map[int16]int8

	StrInfo StrInfoTable
	IntInfo IntInfoTable
}

type Table struct {
	SavedTableInfo

	BlockInfoCache map[string]*SavedColumnInfo
	NewBlockInfos  []string

	BlockList map[string]*TableBlock
	// Need to keep track of the last block we've used, right?
	LastBlock TableBlock
	RowBlock  *TableBlock

	// List of new records that haven't been saved to file yet
	NewRecords RecordList

	KeyStringIDLookup map[int16]string
	ValStringIDLookup map[int32]string

	// This is used for join tables
	JoinLookup map[string]*Record

	StringIDMutex *sync.RWMutex
	RecordMutex   *sync.Mutex
	BlockMutex    *sync.Mutex
}

func InitDataStructures(t *Table) {
	t.KeyStringIDLookup = make(map[int16]string)
	t.ValStringIDLookup = make(map[int32]string)

	t.KeyTable = make(map[string]int16)
	t.KeyTypes = make(map[int16]int8)

	t.BlockList = make(map[string]*TableBlock, 0)

	t.BlockInfoCache = make(map[string]*SavedColumnInfo, 0)
	t.NewBlockInfos = make([]string, 0)

	t.StrInfo = make(StrInfoTable)
	t.IntInfo = make(IntInfoTable)

	t.LastBlock = NewTableBlock()
	t.LastBlock.RecordList = t.NewRecords

	t.StringIDMutex = &sync.RWMutex{}
	t.RecordMutex = &sync.Mutex{}
	t.BlockMutex = &sync.Mutex{}

}

// Remove our pointer to the blocklist so a GC is triggered and
// a bunch of new memory becomes available
func ReleaseRecords(t *Table) {
	t.BlockList = make(map[string]*TableBlock, 0)
	debug.FreeOSMemory()
}

func ResetBlockCache(t *Table) {
	t.BlockInfoCache = make(map[string]*SavedColumnInfo, 0)
}

var LOADED_TABLES = make(map[string]*Table)

var table_m sync.Mutex

// This is a singleton constructor for Tables
func GetTable(name string) *Table {
	table_m.Lock()
	defer table_m.Unlock()

	t, ok := LOADED_TABLES[name]
	if ok {
		return t
	}

	t = &Table{}
	t.Name = name

	LOADED_TABLES[name] = t

	InitDataStructures(t)

	return t
}

func MakeDir(t *Table) {
	tabledir := path.Join(*FLAGS.DIR, t.Name)
	os.MkdirAll(tabledir, 0755)
}

func IsNotExist(t *Table) bool {
	table_dir := path.Join(*FLAGS.DIR, t.Name)
	_, err := ioutil.ReadDir(table_dir)
	return err != nil
}
