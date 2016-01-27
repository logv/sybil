package edb

import "fmt"
import "sync"
import "flag"
import "time"

var f_RESET = flag.Bool("reset", false, "Reset the DB")
var f_TABLE = flag.String("table", "", "Table to operate on")
var f_ADD_RECORDS = flag.Int("add", 0, "Add data?")
var f_PRINT = flag.Bool("print", false, "Print some records")
var f_PRINT_INFO = flag.Bool("info", false, "Print table info")

func make_records(name string) {
  fmt.Println("Adding", *f_ADD_RECORDS, "to", name)
  for i := 0; i < *f_ADD_RECORDS; i++ {
    NewRandomRecord(name); 
  }

}

func add_records() {
  if (*f_ADD_RECORDS == 0) {
    return
  }


  fmt.Println("MAKING RECORDS FOR TABLE", *f_TABLE)
  if *f_TABLE != "" {
    make_records(*f_TABLE)
    return
  }

  var wg sync.WaitGroup
  for j := 0; j < 10; j++ {
    wg.Add(1)
    q := j
    go func() {
      defer wg.Done()
      table_name := fmt.Sprintf("test%v", q)
      make_records(table_name)
    }()
  }

  wg.Wait()

}

func testTable(name string) {
  table := getTable(name)

  load_spec := NewLoadSpec()
  load_spec.Int("age")
  load_spec.Str("state")
  load_spec.Str("session_id")

  lstart := time.Now()
  table.LoadRecords(&load_spec)
  lend := time.Now()
  fmt.Println("LOADING RECORDS INTO TABLE TOOK", lend.Sub(lstart))

  filters := []Filter{}

  start := time.Now()
  ret := table.MatchRecords(filters)
  end := time.Now()
  fmt.Println("NO FILTER RETURNED", len(ret), "RECORDS, TOOK", end.Sub(start))

  age_filter := table.IntFilter("age", "lt", 20)
  filters = append(filters, age_filter)

  start = time.Now()
  filt_ret := table.MatchRecords(filters)
  end = time.Now()
  fmt.Println("INT FILTER RETURNED", len(filt_ret), "RECORDS, TOOK", end.Sub(start))

  table.AggRecords(ret)
  table.AggRecords(filt_ret)


  start = time.Now()
  session_maps := SessionizeRecords(ret, "session_id")
  end = time.Now()
  fmt.Println("SESSIONIZED", len(ret), "RECORDS INTO", len(session_maps), "SESSIONS, TOOK", end.Sub(start))

  start = time.Now()
  session_maps = SessionizeRecords(filt_ret, "session_id")
  end = time.Now()
  fmt.Println("SESSIONIZED", len(filt_ret), "RECORDS INTO", len(session_maps), "SESSIONS, TOOK", end.Sub(start))
}

func Start() {
  flag.Parse()

  fmt.Println("Starting DB")
  fmt.Println("TABLE", *f_TABLE);


  add_records()

  table := *f_TABLE
  if table == "" { table = "test0" }

  start := time.Now()
  testTable(table)
  end := time.Now()
  fmt.Println("TESTING TABLE TOOK", end.Sub(start))

  start = time.Now()
  SaveTables()
  end = time.Now()
  fmt.Println("SERIALIZED DB TOOK", end.Sub(start))

  if *f_PRINT {
    t := getTable(table)
    count := 0
    for _, b := range t.BlockList {
      for _, r := range b.RecordList {
        count++
        t.PrintRecord(r)
        if count > 10 {
          break
        }
      }

      if count > 10 {
        break
      }

    }

  }

  if *f_PRINT_INFO {
    t := getTable(table)
    t.PrintColInfo()
  }

}
