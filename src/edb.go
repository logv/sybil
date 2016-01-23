package edb

import "fmt"
import "sync"
import "math/rand"
import "github.com/Pallinder/go-randomdata"
import "time"
import "strconv"

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
  for i := 0; i < 100000; i++ {
    NewRandomRecord(name); 
  }

}

func load_or_create_records() {
  start := time.Now()
  tables := LoadTables()
  end := time.Now()

  if len(tables) > 0 {
    fmt.Println("LOADED DB, TOOK", end.Sub(start))
    return
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

  ret = table.MatchRecords(filters)
  end = time.Now()
  fmt.Println("INT FILTER RETURNED", len(ret), "RECORDS, TOOK", end.Sub(start))

  start = time.Now()
  session_maps := SessionizeRecords(ret, "session_id")
  end = time.Now()
  fmt.Println("SESSIONIZED", len(session_maps), "SESSIONS, TOOK", end.Sub(start))

  start = time.Now()
  SaveTables()
  end = time.Now()
  fmt.Println("SERIALIZED DB TOOK", end.Sub(start))
}
