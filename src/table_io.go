package edb

import "fmt"
import "log"
import "time"
import "os"
import "strings"
import "bytes"
import "io/ioutil"
import "encoding/gob"
import "sync"

var DEBUG_TIMING = false

type LoadSpec struct {
  columns map[string]bool 
}

func NewLoadSpec() LoadSpec {
  l := LoadSpec{}
  l.columns = make(map[string]bool)

  l.Int("time")
  return l;
}

func (l *LoadSpec) Str(name string) {
  l.columns["str_" + name + ".db"] = true
}
func (l *LoadSpec) Int(name string) {
  l.columns["int_" + name + ".db"] = true
}
func (l *LoadSpec) Set(name string) {
  l.columns["set_" + name + ".db"] = true
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
        table.LoadRecords(nil);
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

  temp_block.SaveToColumns(filename)
}


func (t *Table) FillPartialBlock() bool {
  if len (t.newRecords) == 0 {
    return false
  }

  fmt.Println("CHECKING FOR PARTIAL BLOCK", t.LastBlockId)

  // Open up our last record block, see how full it is
  filename := getBlockDir(t.Name, t.LastBlockId)

  partialRecords := t.LoadBlockFromDir(filename, nil)
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

  chunk_size := CHUNK_SIZE
  chunks := len(records) / chunk_size

  if (chunks == 0) {
    filename := getBlockFilename(t.Name, t.LastBlockId)
    t.SaveRecordsToFile(records, filename)
  } else {
    for j := 0; j < chunks; j++ {
      filename := getBlockFilename(t.Name, t.LastBlockId)
      t.SaveRecordsToFile(records[j*chunk_size:(j+1)*chunk_size], filename)
      t.LastBlockId++
    }

    // SAVE THE REMAINDER
    if len(records) > chunks * chunk_size {
      filename := getBlockFilename(t.Name, t.LastBlockId)
      t.SaveRecordsToFile(records[chunks * chunk_size:], filename)
    }
  }

  fmt.Println("LAST TABLE BLOCK IS", t.LastBlockId)

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

// TODO: have this only pull the blocks into column format and not materialize
// the columns immediately
func (t *Table) LoadBlockFromDir(dirname string, load_spec *LoadSpec) []*Record {
  tb := newTableBlock()
  tb.Name = dirname

  t.block_m.Lock()
  t.BlockList[dirname] = &tb
  t.block_m.Unlock()

  tb.table = t

  // find out how many records are kept in this dir...
  info := SavedColumnInfo{}
  istart := time.Now()
  filename := fmt.Sprintf("%s/info.db", dirname)
  file, _ := os.Open(filename)
  dec := gob.NewDecoder(file)
  dec.Decode(&info)
  iend := time.Now()

  if DEBUG_TIMING {
    fmt.Println("LOAD BLOCK INFO TOOK", iend.Sub(istart))
  }

  start := time.Now()
  end := time.Now()


  var r *Record

  mstart := time.Now()
  records := make([]*Record, info.NumRecords)
  alloced := make([]Record, info.NumRecords)
  bigIntArr := make(IntArr, len(t.KeyTable) * int(info.NumRecords))
  bigStrArr := make(StrArr, len(t.KeyTable) * int(info.NumRecords))
  bigPopArr := make([]int, len(t.KeyTable) * int(info.NumRecords))
  mend := time.Now()

  if DEBUG_TIMING {
    fmt.Println("MALLOCED RECORDS", info.NumRecords, "TOOK", mend.Sub(mstart))
  }

  start = time.Now()
  for i, _ := range records {
    r = &alloced[i]
    r.Ints = bigIntArr[i*len(t.KeyTable):(i+1)*len(t.KeyTable)]
    r.Strs = bigStrArr[i*len(t.KeyTable):(i+1)*len(t.KeyTable)]
    r.Populated = bigPopArr[i*len(t.KeyTable):(i+1)*len(t.KeyTable)]

    r.block = &tb
    records[i] = r
  }
  end = time.Now()

  if DEBUG_TIMING {
    fmt.Println("INITIALIZED RECORDS", info.NumRecords, "TOOK", end.Sub(start))
  }

  file, _ = os.Open(dirname)
  files, _ := file.Readdir(-1)

  for _, f := range files {
    fname := f.Name()

    if load_spec != nil {
      if load_spec.columns[fname] != true {
	continue
      }
    }

    filename := fmt.Sprintf("%s/%s", dirname, fname)

    file, _ := os.Open(filename)
    dec := gob.NewDecoder(file)
    switch {
      case strings.HasPrefix(fname, "str"):
        into := &SavedStrs{}
        err := dec.Decode(into);
        string_lookup := make(map[int32]string)

        if err != nil { fmt.Println("DECODE COL ERR:", err) }
  
        col := tb.getColumnInfo(into.Name)
	// unpack the string table
	for k, v := range into.StringTable {
	  col.StringTable[v] = int32(k) 
	  string_lookup[int32(k)] = v
	}
        col.val_string_id_lookup = string_lookup

        for _, bucket := range into.Bins {

          for _, r := range bucket.Records {
            val :=  string_lookup[bucket.Value]
            value_id := col.get_val_id(val)

            records[r].Strs[into.Name] = StrField(value_id)
	    records[r].Populated[into.Name] = STR_VAL
          }
        }

      case strings.HasPrefix(fname, "int"):
        into := &SavedInts{}
        err := dec.Decode(into);
        if err != nil { fmt.Println("DECODE COL ERR:", err) }
        for _, bucket := range into.Bins {
          for _, r := range bucket.Records {

            records[r].Ints[into.Name] = IntField(bucket.Value)
	    records[r].Populated[into.Name] = INT_VAL
            tb.table.update_int_info(into.Name, int(bucket.Value))
          }


        }
    }
  }


  tb.RecordList = records[:]
  return records[:]
}

func (t *Table) LoadRecords(load_spec *LoadSpec) {
  waystart := time.Now()
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

  wg.Wait()

  fmt.Println("KEY TABLE", t.KeyTable)

  m := &sync.Mutex{}

  count := 0
  var records []*Record
  for _, v := range files {
    if strings.HasSuffix(v.Name(), "info.db") {
      continue
    }

    if v.IsDir() {
      filename := fmt.Sprintf("db/%s/%s", t.Name, v.Name())
      wg.Add(1)
      go func() {
        defer wg.Done()
	start := time.Now()
        records = t.LoadBlockFromDir(filename, load_spec);
	end := time.Now()
	fmt.Println("LOADED BLOCK FROM DIR", filename, "TOOK", end.Sub(start))
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

  fmt.Println("LOADED", count, "RECORDS INTO", t.Name, "TOOK", end.Sub(waystart));
}

