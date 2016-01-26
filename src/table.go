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

type Table struct {
  Name string;
  BlockList map[string]TableBlock
  KeyTable map[string]int16 // String Key Names

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
  t.BlockList = make(map[string]TableBlock, 0)

  string_table := make(map[string]int32)
  t.LastBlock = newTableBlock()
  t.LastBlock.RecordList = t.newRecords
  t.LastBlock.StringTable = string_table

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

func (t *Table) SaveTableInfo(fname string) {
  var network bytes.Buffer // Stand-in for the network.
  filename := fmt.Sprintf("db/%s/%s.db", t.Name, fname)

  // Create an encoder and send a value.
  enc := gob.NewEncoder(&network)
  err := enc.Encode(t)

  if err != nil {
    log.Fatal("encode:", err)
  }

  fmt.Println("SERIALIZED TABLE INFO", fname, "INTO BYTES", network.Len(), "BYTES");

  w, _ := os.Create(filename)
  network.WriteTo(w);


}

func (t *Table) SaveRecordsToFile(records []*Record, filename string) {
  if len(records) == 0 {
    return
  }

  temp_block := newTableBlock()
  temp_block.RecordList = records
  temp_block.table = t

  temp_block.SaveToFile(filename)
  temp_block.SaveToColumns(filename)
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
    LastBlockId: t.LastBlockId}
}

func (t *Table) saveRecordList(records []*Record) bool {
  if (!t.dirty) { return false; }

  fmt.Println("SAVING RECORD LIST", len(records))

  save_table := getSaveTable(t)
  save_table.SaveTableInfo("info")

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
  save_table.SaveTableInfo("info")

  t.dirty = false;

  return true;
}

func (t *Table) SaveRecords() bool {
  os.MkdirAll(fmt.Sprintf("db/%s", t.Name), 0777)
  t.FillPartialBlock()
  return t.saveRecordList(t.newRecords)
}

func LoadTableInfo(tablename, fname string) *Table {
  t := Table{}
  start := time.Now()
  filename := fmt.Sprintf("db/%s/%s.db", tablename, fname)
  file, _ := os.Open(filename)
  fmt.Println("OPENING TABLE INFO FROM FILENAME", filename)
  dec := gob.NewDecoder(file)
  err := dec.Decode(&t);
  end := time.Now()
  if err != nil {
    fmt.Println("TABLE INFO DECODE:", err);
    return &t;
  }

  fmt.Println("TABLE INFO", fname, "OPEN TOOK", end.Sub(start))

  return &t
}

func (t *Table) LoadBlockFromFile(filename string) *TableBlock {
  start := time.Now()
  file, _ := os.Open(filename)
  var saved_block = SavedBlock{}
  var records []*Record
  dec := gob.NewDecoder(file)
  err := dec.Decode(&saved_block);
  end := time.Now()
  if err != nil {
    fmt.Println("DECODE ERR:", err);
    return nil;
  }
  fmt.Println("DECODED RECORDS FROM FILENAME", filename, "TOOK", end.Sub(start))


  records = make([]*Record, len(saved_block.Records))
  tb := newTableBlock()
  tb.RecordList = records
  tb.table = t
  tb.StringTable = saved_block.StringTable

  for i, s := range saved_block.Records {
    records[i] = s.toRecord(&tb)
  }

  t.record_m.Lock()
  t.BlockList[filename] = tb
  t.record_m.Unlock()

  return &tb

}

func (t *Table) LoadRecordsFromFile(filename string) []*Record {
  tb := t.LoadBlockFromFile(filename)
  if tb == nil {
    var records []*Record
    return records

  }

  return tb.RecordList
}

func (t *Table) LoadRecords() {
  start := time.Now()
  fmt.Println("LOADING", t.Name)

  files, _ := ioutil.ReadDir(fmt.Sprintf("db/%s/", t.Name))

  var wg sync.WaitGroup
  
  wg.Add(1)
  // why is table info so slow to open!!!
  go func() { 
    defer wg.Done()
    saved_table := LoadTableInfo(t.Name, "info")
    if saved_table.KeyTable != nil && len(saved_table.KeyTable) > 0 {
      t.KeyTable = saved_table.KeyTable
    }
    t.LastBlockId = saved_table.LastBlockId
  }()

  m := &sync.Mutex{}

  count := 0
  for _, v := range files {
    if strings.HasSuffix(v.Name(), "info.db") {
      continue
    }

    if strings.HasSuffix(v.Name(), "strings.db") {
      continue
    }

    if strings.HasSuffix(v.Name(), ".db") {
      filename := fmt.Sprintf("db/%s/%s", t.Name, v.Name())
      wg.Add(1)
      go func() {
        defer wg.Done()
        records := t.LoadRecordsFromFile(filename);
        if len(records) > 0 {
          m.Lock()
          count += len(records)
          m.Unlock()
        }
      }()
    }

  }

  wg.Wait()


  // RE-POPULATE LOOKUP TABLE INFO
  t.populate_string_id_lookup();


  end := time.Now()

  fmt.Println("LOADED", count, "RECORDS INTO", t.Name, "TOOK", end.Sub(start));
}

func (t *Table) get_string_for_key(id int16) string {
  val, _ := t.key_string_id_lookup[id];
  return val
}

func (t *Table) populate_string_id_lookup() {
  t.string_id_m.Lock()
  defer t.string_id_m.Unlock()

  t.key_string_id_lookup = make(map[int16]string)
  t.val_string_id_lookup = make(map[int32]string)

  for k, v := range t.KeyTable { t.key_string_id_lookup[v] = k; }

  for _, b := range t.BlockList {
    for k, v := range b.StringTable { b.val_string_id_lookup[v] = k; }

  }
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

  t.record_m.Lock();
  t.newRecords = append(t.newRecords, &r)
  t.record_m.Unlock();
  return &r
}

func (t *Table) PrintRecord(r *Record) {
  fmt.Println("RECORD", r);
  fmt.Println("STRING ID LOOKUP", t.KeyTable)
  for name, val := range r.Ints {
    fmt.Println("  ", t.get_string_for_key(name), val);
  }
  for name, val := range r.Strs {
    fmt.Println("  ", t.get_string_for_key(name), r.block.columns[name].get_string_for_val(int32(val)));
  }
}

func (t *Table) PrintRecords(records []*Record) {
  for i := 0; i < len(records); i++ {
    t.PrintRecord(records[i])
  }
}

func (t *Table) PrintColInfo() {
  for k, v := range t.int_info_table {
    fmt.Println(k, v)
  }

}

