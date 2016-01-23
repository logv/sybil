package edb

import "fmt"
import "sync"
import "flag"
import "math/rand"
import "github.com/Pallinder/go-randomdata"
import "time"
import "strconv"


var f_RESET = flag.Bool("reset", false, "Reset the DB")
var f_TABLE = flag.String("table", "test0", "Table to operate on")
var f_ADD_DATA = flag.Bool("add", false, "Add data?")
var f_NEW_RECORDS = flag.Int("n", 10000, "number of records to add")


func NewRandomRecord(table_name string) *Record {
  t := getTable(table_name)
  r := t.NewRecord()
  r.AddIntField("age", rand.Intn(50) + 10)
  r.AddIntField("time", int(time.Now().Unix()))
  r.AddStrField("name", randomdata.FirstName(randomdata.RandomGender))
  r.AddStrField("friend", randomdata.FirstName(randomdata.RandomGender))
  r.AddStrField("enemy", randomdata.FirstName(randomdata.RandomGender))
  r.AddStrField("event", randomdata.City())
  r.AddStrField("session_id", strconv.FormatInt(int64(rand.Intn(5000)), 16))

  return r;

}

func make_records(name string) {
  fmt.Println("Adding", *f_NEW_RECORDS, "to", name)
  for i := 0; i < *f_NEW_RECORDS; i++ {
    NewRandomRecord(name); 
  }

}

func load_or_create_records() {
  start := time.Now()

  tables := make([]Table, 0)
  if (*f_RESET == false) {
    tables = LoadTables()
  }
  end := time.Now()

  if len(tables) > 0 {
    fmt.Println("LOADED DB, TOOK", end.Sub(start))

    if (*f_ADD_DATA == false) {
      return
    }
  }

  start = time.Now()

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
  end = time.Now()
  t := getTable("test0")

  fmt.Println("CREATED RECORDS", len(t.RecordList), "TOOK", end.Sub(start))

}

func Start() {
  flag.Parse()

  fmt.Println("Starting DB")

  load_or_create_records()
  table := getTable("test0")

  start := time.Now()
  filters := []Filter{}

  ret := table.MatchRecords(filters)
  end := time.Now()
  fmt.Println("NO FILTER RETURNED", len(ret), "RECORDS, TOOK", end.Sub(start))

  age_filter := table.IntFilter("age", "lt", 20)
  filters = append(filters, age_filter)

  filt_ret := table.MatchRecords(filters)
  end = time.Now()
  fmt.Println("INT FILTER RETURNED", len(filt_ret), "RECORDS, TOOK", end.Sub(start))

  start = time.Now()
  session_maps := SessionizeRecords(ret, "session_id")
  end = time.Now()
  fmt.Println("SESSIONIZED", len(ret), "RECORDS INT", len(session_maps), "SESSIONS, TOOK", end.Sub(start))

  start = time.Now()
  session_maps = SessionizeRecords(filt_ret, "session_id")
  end = time.Now()
  fmt.Println("SESSIONIZED", len(filt_ret), "RECORDS INT", len(session_maps), "SESSIONS, TOOK", end.Sub(start))

  start = time.Now()
  SaveTables()
  end = time.Now()
  fmt.Println("SERIALIZED DB TOOK", end.Sub(start))
}
