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
import "sort"
import "runtime/debug"
import "strconv"

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



// Helpers for block directory structure
func getBlockName(id int) string {
  return strconv.FormatInt(int64(id), 10)
}

func getBlockDir(name string, id int) string {
  return fmt.Sprintf("db/%s/%05s", name, getBlockName(id))
}
func getBlockFilename(name string, id int) string {
  return fmt.Sprintf("db/%s/%05s.db", name, getBlockName(id))
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

  partialRecords := t.LoadBlockFromDir(filename, nil, true /* LOAD ALL RECORDS */)
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
  if len(records) == 0 {
    return false
  }

  fmt.Println("SAVING RECORD LIST", len(records), t.Name)

  save_table := getSaveTable(t)
  save_table.SaveTableInfo("info")

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
  sort.Sort(SortRecordsByTime(t.newRecords))

  t.FillPartialBlock()
  return t.saveRecordList(t.newRecords)
}

func (t *Table) LoadTableInfo() {
  saved_table := Table{}
  start := time.Now()
  tablename := t.Name
  filename := fmt.Sprintf("db/%s/info.db", tablename)
  file, _ := os.Open(filename)
  fmt.Println("OPENING TABLE INFO FROM FILENAME", filename)
  dec := gob.NewDecoder(file)
  err := dec.Decode(&saved_table);
  end := time.Now()
  if err != nil {
    fmt.Println("TABLE INFO DECODE:", err);
    return
  }

  fmt.Println("TABLE INFO OPEN TOOK", end.Sub(start))

  if t.KeyTable != nil && len(saved_table.KeyTable) > 0 {
    t.KeyTable = saved_table.KeyTable
  }
  t.LastBlockId = saved_table.LastBlockId

  return
}

// TODO: have this only pull the blocks into column format and not materialize
// the columns immediately
func (t *Table) LoadBlockFromDir(dirname string, load_spec *LoadSpec, load_records bool) []*Record {
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

  var records []*Record
  var alloced []Record
  var bigIntArr IntArr
  var bigStrArr StrArr
  var bigPopArr []int
  max_key_id := 0
  for _, v := range t.KeyTable {
    if max_key_id <= int(v) {
      max_key_id = int(v) + 1
    }
  }

  if load_spec != nil || load_records {
    mstart := time.Now()
    records = make([]*Record, info.NumRecords)
    alloced = make([]Record, info.NumRecords)
    bigIntArr = make(IntArr, max_key_id * int(info.NumRecords))
    bigStrArr = make(StrArr, max_key_id * int(info.NumRecords))
    bigPopArr = make([]int, max_key_id * int(info.NumRecords))
    mend := time.Now()

    if DEBUG_TIMING {
      fmt.Println("MALLOCED RECORDS", info.NumRecords, "TOOK", mend.Sub(mstart))
    }

    start = time.Now()
    for i, _ := range records {
      r = &alloced[i]
      r.Ints = bigIntArr[i*max_key_id:(i+1)*max_key_id]
      r.Strs = bigStrArr[i*max_key_id:(i+1)*max_key_id]
      r.Populated = bigPopArr[i*max_key_id:(i+1)*max_key_id]

      r.block = &tb
      records[i] = r
    }
    end = time.Now()

    if DEBUG_TIMING {
      fmt.Println("INITIALIZED RECORDS", info.NumRecords, "TOOK", end.Sub(start))
    }
  }

  file, _ = os.Open(dirname)
  files, _ := file.Readdir(-1)

  for _, f := range files {
    fname := f.Name()

    if load_spec != nil {
      if load_spec.columns[fname] != true {
	continue
      }
    } else if load_records == false {
      continue
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

	var record *Record
        for _, bucket := range into.Bins {

          for _, r := range bucket.Records {
            val :=  string_lookup[bucket.Value]
            value_id := col.get_val_id(val)

	    record = records[r]

            record.Strs[into.Name] = StrField(value_id)
	    record.Populated[into.Name] = STR_VAL
          }
        }

      case strings.HasPrefix(fname, "int"):
        into := &SavedInts{}
        err := dec.Decode(into);
        if err != nil { fmt.Println("DECODE COL ERR:", err) }
        for _, bucket := range into.Bins {
	  tb.table.update_int_info(into.Name, int(bucket.Value))

          for _, r := range bucket.Records {

            records[r].Ints[into.Name] = IntField(bucket.Value)
	    records[r].Populated[into.Name] = INT_VAL
          }


        }
    }
  }


  tb.RecordList = records[:]

  return records[:]
}

// Remove our pointer to the blocklist so a GC is triggered and
// a bunch of new memory becomes available
func (t *Table) ReleaseRecords() {
  t.BlockList = make(map[string]*TableBlock, 0)
}

func (t *Table) LoadRecords(load_spec *LoadSpec) {
  waystart := time.Now()
  fmt.Println("LOADING", t.Name)

  // turn off the gc (and turn it back on after this func) because
  // MALLOC + GC is slow with millions of records
  debug.SetGCPercent(-1)
  defer debug.SetGCPercent(100)

  files, _ := ioutil.ReadDir(fmt.Sprintf("db/%s/", t.Name))

  var wg sync.WaitGroup

  wg.Add(1)
  // why is table info so slow to open!!!
  go func() {
    defer wg.Done()
    t.LoadTableInfo()
  }()

  wg.Wait()


  m := &sync.Mutex{}

  count := 0
  var records []*Record
  for _, v := range files {
    if strings.HasSuffix(v.Name(), "info.db") {
      continue
    }

    if strings.HasSuffix(v.Name(), "partial") {
      continue
    }

    if v.IsDir() {
      filename := fmt.Sprintf("db/%s/%s", t.Name, v.Name())
      wg.Add(1)
      go func() {
        defer wg.Done()
	start := time.Now()
        records = t.LoadBlockFromDir(filename, load_spec, false);
	end := time.Now()
	if load_spec != nil {
	  fmt.Println("LOADED BLOCK FROM DIR", filename, "TOOK", end.Sub(start))
	} else {
	  fmt.Println("LOADED INFO FOR BLOCK", filename, "TOOK", end.Sub(start))
	}

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

  if load_spec != nil {
    fmt.Println("LOADED", count, "RECORDS INTO", t.Name, "TOOK", end.Sub(waystart));
  } else {
    fmt.Println("INSPECTED", len(t.BlockList), "BLOCKS", "TOOK", end.Sub(waystart))
  }
}

