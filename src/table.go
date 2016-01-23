package edb

import "fmt"
import "log"
import "os"
import "math"
import "strings"
import "sync"
import "bytes"
import "io/ioutil"
import "encoding/gob"

type Table struct {
  Name string;
  RecordList []*Record
  StringTable map[string]int

  dirty bool;
  string_id_lookup map[int]string
  string_id_m *sync.Mutex;
  record_m *sync.Mutex;
}

var LOADED_TABLES = make(map[string]*Table);


// This is a singleton constructor for Tables
func getTable(name string) *Table{
  
  t, ok := LOADED_TABLES[name]
  if ok {
    return t;
  }

  t = &Table{Name: name, dirty: false}
  LOADED_TABLES[name] = t
  t.string_id_lookup = make(map[int]string)
  t.StringTable = make(map[string]int)
  t.RecordList = make([]*Record, 0)
  t.string_id_m = &sync.Mutex{}
  t.record_m = &sync.Mutex{}


  return t;
}


func LoadTables() []Table {
  files, _ := ioutil.ReadDir("db/")

  for _, v := range files {
    if strings.HasSuffix(v.Name(), ".db") {
      name := strings.TrimSuffix(v.Name(), ".db")
      table := getTable(name)
      table.LoadRecords();
    }

  }

  tables := make([]Table, len(LOADED_TABLES))
  for _, v := range LOADED_TABLES {
    tables = append(tables, *v);
  }
  return tables
  
}

func SaveTables() {
  for _, t := range LOADED_TABLES {
    t.SaveRecords();
  }

}

func (t *Table) SaveRecords() bool {
  if (!t.dirty) { return false; }

  fmt.Println("SAVING TABLE", t.Name);

  var network bytes.Buffer // Stand-in for the network.

  // Create an encoder and send a value.
  enc := gob.NewEncoder(&network)
  err := enc.Encode(t)

  if err != nil {
    log.Fatal("encode:", err)
  }

  length := int(math.Max(float64(len(t.RecordList)), 1.0));
  fmt.Println("SERIALIZED INTO BYTES", network.Len(), "BYTES", "( PER RECORD", network.Len() / length, ")");

  w, _ := os.Create(fmt.Sprintf("db/%s.db", t.Name))
  network.WriteTo(w);

  t.dirty = false;

  return true;


}

func (t *Table) LoadRecords() {
  file, _ := os.Open(fmt.Sprintf("db/%s.db", t.Name))
  dec := gob.NewDecoder(file)
  err := dec.Decode(t);
  if err != nil {
    fmt.Println("DECODE:", err);
    return ;
  }

  t.populate_string_id_lookup();
  for _, r := range(t.RecordList) {
    r.table = t;
  }
  fmt.Println("LOADED", len(t.RecordList), "RECORDS INTO", t.Name);
}

func (t *Table) get_string_from_id(id int) string {
  val, _ := t.string_id_lookup[id];
  return val
}

func (t *Table) populate_string_id_lookup() {
  t.string_id_lookup = make(map[int]string)

  for k, v := range t.StringTable {
    t.string_id_lookup[v] = k; 
  }
}

func (t *Table) get_string_id(name string) int {
  t.string_id_m.Lock();
  defer t.string_id_m.Unlock();
  id, ok := t.StringTable[name]

  if ok {
    return id;
  }


  t.StringTable[name] = len(t.StringTable);
  t.string_id_lookup[t.StringTable[name]] = name;
  return t.StringTable[name];
}



func (t *Table) NewRecord() *Record {  
  r := Record{ Sets: SetArr{}, Ints: IntArr{}, Strs: StrArr{} }
  t.dirty = true;
  r.table = t;

  t.record_m.Lock();
  t.RecordList = append(t.RecordList, &r)
  t.record_m.Unlock();
  return &r
}


func (t *Table) MatchRecords(filters []Filter) []*Record {
  ret := make([]*Record, 0);

  for i := 0; i < len(t.RecordList); i++ {
    add := true;
    r := t.RecordList[i];

    for j := 0; j < len(filters); j++ { 
      if filters[j].Filter(*r) {
        add = false;
        break;
      }
    }

    if add {
      ret = append(ret, r);
    }
  }

  return ret;
}


func (t *Table) PrintRecords() {
  for i := 0; i < len(t.RecordList); i++ {
    fmt.Println("\nRECORD");
    r := t.RecordList[i]
    fmt.Println(r)
    for _, val := range r.Ints {
      fmt.Println("  ", t.get_string_from_id(val.Name), val.Value);
    }
    for _, val := range r.Strs {
      fmt.Println("  ", t.get_string_from_id(val.Name), t.get_string_from_id(val.Value));
    }
  }
}

