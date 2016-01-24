package edb

import "fmt"
import "log"
import "time"
import "os"
import "math"
import "strings"
import "strconv"
import "sync"
import "bytes"
import "io/ioutil"
import "encoding/gob"

type Table struct {
  Name string;
  RecordList []*Record
  StringTable map[string]int

  // Need to keep track of the last block we've used, right?
  LastBlockId int

  // List of new records that haven't been saved to file yet
  newRecords []*Record

  dirty bool;
  string_id_lookup map[int]string
  string_id_m *sync.Mutex;
  record_m *sync.Mutex;
}

var LOADED_TABLES = make(map[string]*Table);

var CHUNK_SIZE = 1024 * 64;


var table_m sync.Mutex

func getBlockName(id int) string {
  return strconv.FormatInt(int64(id), 10)
}

func getBlockFilename(name string, id int) string {
  return fmt.Sprintf("db/%s/%05s.db", name, getBlockName(id))
}


// This is a singleton constructor for Tables
func getTable(name string) *Table{
  table_m.Lock()
  defer table_m.Unlock()

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
  t.LoadRecords();


  return t;
}


func LoadTables() []Table {
  files, _ := ioutil.ReadDir("db/")

  var wg sync.WaitGroup
  for _, v := range files {
    if strings.HasSuffix(v.Name(), ".db") {
      wg.Add(1)
      name := strings.TrimSuffix(v.Name(), ".db")
      table := getTable(name)
      go func() {
        defer wg.Done()
        table.LoadRecords();
      }()
    }


  }

  wg.Wait()

  tables := make([]Table, len(LOADED_TABLES))
  for _, v := range LOADED_TABLES {
    tables = append(tables, *v);
  }
  return tables
  
}

func SaveTables() {
  var wg sync.WaitGroup
  for _, t := range LOADED_TABLES {
    wg.Add(1)
    qt := t
    go func() {
      defer wg.Done()
      qt.SaveRecords();
    }()
  }

  wg.Wait()

}

func (t *Table) SaveTableInfo() {
  var network bytes.Buffer // Stand-in for the network.
  filename := fmt.Sprintf("db/%s/info.db", t.Name)

  // Create an encoder and send a value.
  enc := gob.NewEncoder(&network)
  err := enc.Encode(t)

  if err != nil {
    log.Fatal("encode:", err)
  }

  length := int(math.Max(float64(len(t.RecordList)), 1.0));
  fmt.Println("SERIALIZED TABLE INFO INTO BYTES", network.Len(), "BYTES", "( PER RECORD", network.Len() / length, ")");

  w, _ := os.Create(filename)
  network.WriteTo(w);


}

func (t *Table) SaveRecordsToFile(records []*Record, filename string) {
  if len(records) == 0 {
    return
  }

  var network bytes.Buffer // Stand-in for the network.

  // Create an encoder and send a value.
  enc := gob.NewEncoder(&network)
  err := enc.Encode(records)

  if err != nil {
    log.Fatal("encode:", err)
  }

  fmt.Println("SERIALIZED INTO BLOCK", filename, network.Len(), "BYTES", "( PER RECORD", network.Len() / len(records), ")");

  w, _ := os.Create(filename)
  network.WriteTo(w);

}


func (t *Table) FillPartialBlock() bool {
  if len (t.newRecords) == 0 {
    return false
  }

  fmt.Println("CHECKING FOR PARTIAL BLOCK", t.LastBlockId)

  // Open up our last record block, see how full it is
  filename := getBlockFilename(t.Name, t.LastBlockId)
  partialRecords := t.LoadRecordsFromFile(filename)
  fmt.Println("LAST BLOCK HAS", len(partialRecords), "RECORDS")

  incBlockId := false;
  if len(partialRecords) < CHUNK_SIZE {
    delta := CHUNK_SIZE - len(partialRecords)
    if delta > len(t.newRecords) {
      delta = len(t.newRecords)
    } else {
      incBlockId = true
    }

    fmt.Println("SAVING PARTIAL RECORDS", delta, "TO", filename)
    partialRecords = append(partialRecords, t.newRecords[0:delta]...)
    t.SaveRecordsToFile(partialRecords, filename)
    if delta < len(t.newRecords) {
      t.newRecords = t.newRecords[delta:]
    } else {
      t.newRecords = make([]*Record, 0)
    }

  } else {
    incBlockId = true;
  }


  if incBlockId {
    fmt.Println("INC LAST BLOCK")
    t.LastBlockId++
  }

  return true;
}

func (t *Table) saveRecordList(records []*Record) bool {
  if (!t.dirty) { return false; }

  fmt.Println("SAVING RECORD LIST", len(records))

  save_table := Table{Name: t.Name, StringTable: t.StringTable, LastBlockId: t.LastBlockId}
  save_table.SaveTableInfo()

  fmt.Println("SAVING TABLE", t.Name);
  fmt.Println("LAST BLOCK ID", t.LastBlockId)

  chunk_size := CHUNK_SIZE
  chunks := len(records) / chunk_size

  if (chunks == 0) {
    filename := getBlockFilename(t.Name, t.LastBlockId)
    t.SaveRecordsToFile(records, filename)
  } else {
    for j := 0; j < chunks; j++ {
      t.LastBlockId++
      filename := getBlockFilename(t.Name, t.LastBlockId)
      t.SaveRecordsToFile(records[j*chunk_size:(j+1)*chunk_size], filename)
    }

    // SAVE THE REMAINDER
    if len(records) > chunks * chunk_size {
      t.LastBlockId++
      filename := getBlockFilename(t.Name, t.LastBlockId)
      t.SaveRecordsToFile(records[chunks * chunk_size:], filename)
    }
  }

  fmt.Println("LAST BLOCK ID", t.LastBlockId)

  save_table = Table{Name: t.Name, StringTable: t.StringTable, LastBlockId: t.LastBlockId}
  save_table.SaveTableInfo()




  t.dirty = false;

  return true;
}

func (t *Table) SaveAllRecords() bool {
  os.MkdirAll(fmt.Sprintf("db/%s", t.Name), 0777)
  t.LastBlockId = 0;
  return t.saveRecordList(t.RecordList)
}

func (t *Table) SaveRecords() bool {
  os.MkdirAll(fmt.Sprintf("db/%s", t.Name), 0777)
  t.FillPartialBlock()
  return t.saveRecordList(t.newRecords)
}

func (t *Table) LoadTableInfo() {
  filename := fmt.Sprintf("db/%s/info.db", t.Name)
  file, _ := os.Open(filename)
  fmt.Println("OPENING TABLE INFO FROM FILENAME", filename)
  dec := gob.NewDecoder(file)
  err := dec.Decode(t);
  if err != nil {
    fmt.Println("TABLE INFO DECODE:", err);
    return ;
  }

  return 
}

func (t *Table) LoadRecordsFromFile(filename string) []*Record {
  file, _ := os.Open(filename)
  fmt.Println("OPENING RECORDS FROM FILENAME", filename)
  var records []*Record
  dec := gob.NewDecoder(file)
  err := dec.Decode(&records);
  if err != nil {
    fmt.Println("DECODE:", err);
    return records;
  }

  return records



}

func (t *Table) LoadRecords() {
  start := time.Now()
  fmt.Println("LOADING", t.Name)

  files, _ := ioutil.ReadDir(fmt.Sprintf("db/%s/", t.Name))
  ret := []*Record{}
  m := &sync.Mutex{}

  t.LoadTableInfo()

  var wg sync.WaitGroup
  for _, v := range files {
    if strings.HasSuffix(v.Name(), ".db") {
      filename := fmt.Sprintf("db/%s/%s", t.Name, v.Name())
      wg.Add(1)
      go func() {
        defer wg.Done()
        records := t.LoadRecordsFromFile(filename);
        m.Lock()
        ret = append(ret, records...)
        m.Unlock()
      }()
    }

  }

  wg.Wait()


  t.RecordList = ret;
  t.populate_string_id_lookup();
  for _, r := range(t.RecordList) {
    r.table = t;
  }

  end := time.Now()

  fmt.Println("LOADED", len(t.RecordList), "RECORDS INTO", t.Name, "TOOK", end.Sub(start));
}

func (t *Table) get_string_from_id(id int) string {
  val, _ := t.string_id_lookup[id];
  return val
}

func (t *Table) populate_string_id_lookup() {
  t.string_id_m.Lock()
  defer t.string_id_m.Unlock()

  t.string_id_lookup = make(map[int]string)

  for k, v := range t.StringTable {
    t.string_id_lookup[v] = k; 
  }
}

func (t *Table) get_string_id(name string) int {
  id, ok := t.StringTable[name]

  if ok {
    return id;
  }


  t.string_id_m.Lock();
  t.StringTable[name] = len(t.StringTable);
  t.string_id_lookup[t.StringTable[name]] = name;
  t.string_id_m.Unlock();
  return t.StringTable[name];
}



func (t *Table) NewRecord() *Record {  
  r := Record{ Sets: SetArr{}, Ints: IntArr{}, Strs: StrArr{} }
  t.dirty = true;
  r.table = t;

  t.record_m.Lock();
  t.newRecords = append(t.newRecords, &r)
  t.RecordList = append(t.RecordList, &r)
  t.record_m.Unlock();
  return &r
}

func filterRecords(filters []Filter, records []*Record) []*Record{
  ret := make([]*Record, 0);
  for i := 0; i < len(records); i++ {
    add := true;
    r := records[i];

    for j := 0; j < len(filters); j++ { 
      if filters[j].Filter(r) {
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


func (t *Table) MatchRecords(filters []Filter) []*Record {
  ret := make([]*Record, 0);

  var wg sync.WaitGroup

  chunks := 5;
  chunk_size := len(t.RecordList) / chunks
  m := &sync.Mutex{}

  for c := 0; c < chunks; c++ {
    h := c * chunk_size;
    e := (c+1) * chunk_size

    wg.Add(1)
    go func() {
      defer wg.Done()
      records := filterRecords(filters, t.RecordList[h:e])

      m.Lock()
      ret = append(ret, records...)
      m.Unlock()
    }()

  }

  last_records := t.RecordList[chunks * chunk_size:]
  records := filterRecords(filters, last_records)

  m.Lock()
  ret = append(ret, records...)
  m.Unlock()

  wg.Wait()

  return ret
}


func (t *Table) PrintRecords(records []*Record) {
  for i := 0; i < len(records); i++ {
    fmt.Println("\nRECORD");
    r := records[i]
    fmt.Println(r)
    for _, val := range r.Ints {
      fmt.Println("  ", t.get_string_from_id(val.Name), val.Value);
    }
    for _, val := range r.Strs {
      fmt.Println("  ", t.get_string_from_id(val.Name), t.get_string_from_id(val.Value));
    }
  }
}

