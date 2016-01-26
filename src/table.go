package edb

import "fmt"
import "log"
import "time"
import "os"
import "strings"
import "strconv"
import "sync"
import "bytes"
import "io/ioutil"
import "encoding/gob"

type TableBlock struct {
  RecordList []*Record
}
type Table struct {
  Name string;
  BlockList map[string]TableBlock
  KeyTable map[string]int16 // String Key Names
  StringTable map[string]int32 // String Value lookup

  // Need to keep track of the last block we've used, right?
  LastBlockId int
  LastBlock TableBlock

  // List of new records that haven't been saved to file yet
  newRecords []*Record

  dirty bool;
  key_string_id_lookup map[int16]string
  val_string_id_lookup map[int32]string

  int_info_table map[int16]*IntInfo
  string_id_m *sync.Mutex;
  record_m *sync.Mutex;
}

type IntInfo struct {
  Min int
  Max int
  Avg float64
  Count int
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
  t.key_string_id_lookup = make(map[int16]string)
  t.val_string_id_lookup = make(map[int32]string)
  t.int_info_table = make(map[int16]*IntInfo)

  t.KeyTable = make(map[string]int16)
  t.StringTable = make(map[string]int32)
  t.BlockList = make(map[string]TableBlock, 0)
  t.LastBlock = TableBlock{t.newRecords}
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

  fmt.Println("SERIALIZED TABLE INFO INTO BYTES", network.Len(), "BYTES");

  w, _ := os.Create(filename)
  network.WriteTo(w);


}

func (t *Table) SaveRecordsToFile(records []*Record, filename string) {
  if len(records) == 0 {
    return
  }

  marshalled_records := make([]*SavedRecord, len(records))
  for i, r := range records {
    marshalled_records[i] = r.toSavedRecord()
  }

  var network bytes.Buffer // Stand-in for the network.

  // Create an encoder and send a value.
  enc := gob.NewEncoder(&network)
  err := enc.Encode(marshalled_records)

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

func getSaveTable(t *Table) *Table {
  return &Table{Name: t.Name,
    KeyTable: t.KeyTable, 
    StringTable: t.StringTable, 
    LastBlockId: t.LastBlockId}
}

func (t *Table) saveRecordList(records []*Record) bool {
  if (!t.dirty) { return false; }

  fmt.Println("SAVING RECORD LIST", len(records))

  save_table := getSaveTable(t)
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

  save_table = getSaveTable(t)
  save_table.SaveTableInfo()




  t.dirty = false;

  return true;
}

func (t *Table) SaveRecords() bool {
  os.MkdirAll(fmt.Sprintf("db/%s", t.Name), 0777)
  t.FillPartialBlock()
  return t.saveRecordList(t.newRecords)
}

func (t *Table) LoadTableStrings() {
  start := time.Now()
  filename := fmt.Sprintf("db/%s/info.db", t.Name)
  file, _ := os.Open(filename)
  fmt.Println("OPENING TABLE INFO FROM FILENAME", filename)
  dec := gob.NewDecoder(file)
  err := dec.Decode(t);
  end := time.Now()
  if err != nil {
    fmt.Println("TABLE INFO DECODE:", err);
    return ;
  }

  fmt.Println("TABLE INFO OPEN TOOK", end.Sub(start))

  return 
}

func (t *Table) LoadTableInfo() {
  start := time.Now()
  filename := fmt.Sprintf("db/%s/info.db", t.Name)
  file, _ := os.Open(filename)
  fmt.Println("OPENING TABLE INFO FROM FILENAME", filename)
  dec := gob.NewDecoder(file)
  err := dec.Decode(t);
  end := time.Now()
  if err != nil {
    fmt.Println("TABLE INFO DECODE:", err);
    return ;
  }

  fmt.Println("TABLE INFO OPEN TOOK", end.Sub(start))

  return 
}

func (t *Table) LoadRecordsFromFile(filename string) []*Record {
  start := time.Now()
  file, _ := os.Open(filename)
  fmt.Println("OPENING RECORDS FROM FILENAME", filename)
  var marshalled_records []*SavedRecord
  var records []*Record
  dec := gob.NewDecoder(file)
  err := dec.Decode(&marshalled_records);
  end := time.Now()
  if err != nil {
    fmt.Println("DECODE ERR:", err);
    return records;
  }
  fmt.Println("DECODED RECORDS FROM FILENAME", filename, "TOOK", end.Sub(start))


  records = make([]*Record, len(marshalled_records))
  for i, s := range marshalled_records {
    records[i] = s.toRecord(t)
  }

  return records[:]
}

func (t *Table) LoadRecords() {
  start := time.Now()
  fmt.Println("LOADING", t.Name)

  files, _ := ioutil.ReadDir(fmt.Sprintf("db/%s/", t.Name))

  var wg sync.WaitGroup
  
  wg.Add(1)

  // why is table info so slow to open!!!
  go func() { 
    t.LoadTableInfo()
    defer wg.Done()
  }()

  m := &sync.Mutex{}

  for _, v := range files {
    if strings.HasSuffix(v.Name(), ".db") {
      filename := fmt.Sprintf("db/%s/%s", t.Name, v.Name())
      wg.Add(1)
      go func() {
        defer wg.Done()
        records := t.LoadRecordsFromFile(filename);
        if len(records) > 0 {
          block := TableBlock{records}
          m.Lock()
          t.BlockList[filename] = block
          m.Unlock()
        }
      }()
    }

  }

  wg.Wait()


  count := 0
  t.populate_string_id_lookup();
  for _, b := range(t.BlockList) {
    for _, r := range(b.RecordList) {
      r.table = t;
      count++
    }
  }

  end := time.Now()

  fmt.Println("LOADED", count, "RECORDS INTO", t.Name, "TOOK", end.Sub(start));
}

func (t *Table) get_string_for_key(id int16) string {
  val, _ := t.key_string_id_lookup[id];
  return val
}

func (t *Table) get_string_for_val(id int32) string {
  val, _ := t.val_string_id_lookup[id];
  return val
}

func (t *Table) populate_string_id_lookup() {
  t.string_id_m.Lock()
  defer t.string_id_m.Unlock()

  t.key_string_id_lookup = make(map[int16]string)
  t.val_string_id_lookup = make(map[int32]string)

  for k, v := range t.KeyTable { t.key_string_id_lookup[v] = k; }
  for k, v := range t.StringTable { t.val_string_id_lookup[v] = k; }
}

func (t *Table) get_key_id(name string) int16 {
  id, ok := t.KeyTable[name]

  if ok {
    return int16(id);
  }


  t.string_id_m.Lock();
  t.KeyTable[name] = int16(len(t.KeyTable));
  t.key_string_id_lookup[t.KeyTable[name]] = name;
  t.string_id_m.Unlock();
  return int16(t.KeyTable[name]);
}

func (t *Table) get_val_id(name string) int32 {
  id, ok := t.StringTable[name]

  if ok {
    return int32(id);
  }


  t.string_id_m.Lock();
  t.StringTable[name] = int32(len(t.StringTable));
  t.val_string_id_lookup[t.StringTable[name]] = name;
  t.string_id_m.Unlock();
  return t.StringTable[name];
}

func (t *Table) update_int_info(name int16, val int) {
  info, ok := t.int_info_table[name]
  if !ok {
    info = &IntInfo{}
    t.int_info_table[name] = info
    info.Max = val
    info.Min = val
    info.Avg = float64(val)
    info.Count = 1
  }

  if info.Count > 1024 {
    return
  }

  if info.Max < val {
    info.Max = val
  }

  if info.Min > val {
    info.Min = val
  }
  
  info.Avg = info.Avg + (float64(val) - info.Avg) / float64(info.Count)

  info.Count++
}



func (t *Table) NewRecord() *Record {  
  r := Record{ Sets: SetArr{}, Ints: IntArr{}, Strs: StrArr{} }
  t.dirty = true;
  r.table = t;

  t.record_m.Lock();
  t.newRecords = append(t.newRecords, &r)
  t.record_m.Unlock();
  return &r
}

func (t *Table) PrintRecords(records []*Record) {
  for i := 0; i < len(records); i++ {
    fmt.Println("\nRECORD");
    r := records[i]
    for name, val := range r.Ints {
      fmt.Println("  ", t.get_string_for_key(name), val);
    }
    for name, val := range r.Strs {
      fmt.Println("  ", t.get_string_for_key(name), t.get_string_for_val(int32(val)));
    }
  }
}

func (t *Table) PrintColInfo() {
  for k, v := range t.int_info_table {
    fmt.Println(k, v)
  }

}

