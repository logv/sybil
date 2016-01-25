package edb

import "fmt"
import "sync"
import "flag"
import "math/rand"
import "time"
import "strconv"


var f_RESET = flag.Bool("reset", false, "Reset the DB")
var f_TABLE = flag.String("table", "", "Table to operate on")
var f_ADD_RECORDS = flag.Int("add", 0, "Add data?")
var f_PRINT = flag.Bool("print", false, "Print some records")


func NewRandomRecord(table_name string) *Record {
  t := getTable(table_name)
  r := t.NewRecord()
  r.AddIntField("age", rand.Intn(50) + 10)
  r.AddIntField("f1", rand.Intn(50) + 30)
  r.AddIntField("f2", rand.Intn(50) + 2000000)
  r.AddIntField("f3", rand.Intn(50) * rand.Intn(1000) + 10)
  r.AddIntField("time", int(time.Now().Unix()))
  r.AddStrField("session_id", strconv.FormatInt(int64(rand.Intn(500000)), 16))
  r.AddStrField("random_number", strconv.FormatInt(int64(time.Now().Unix() % 20000), 10))

  return r;

}

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

  start = time.Now()
  avg := AvgRecords(filt_ret, "f1")
  end = time.Now()
  fmt.Println("AGG RETURNED", avg, "RECORDS, TOOK", end.Sub(start))


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
    t.PrintRecords(t.RecordList[:10])
  }

}
