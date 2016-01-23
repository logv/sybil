package edb

import "fmt"
import "sync"
import "time"

func make_records() {
  t := getTable("test")
  for i := 0; i < 1000; i++ {
    t.NewRandomRecord(); 
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
    go func() {
      defer wg.Done()
      make_records()
    }()
  }

  wg.Wait()
  end = time.Now()
  t := getTable("test")

  fmt.Println("CREATED RECORDS", len(t.RecordList), "TOOK", end.Sub(start))

}

func Start() {
  fmt.Println("Starting DB")

  load_or_create_records()



  start := time.Now()
  filters := []Filter{NoFilter{}}
  table := getTable("test")

  ret := table.MatchRecords(filters)
  end := time.Now()
  fmt.Println("RETURNED", len(ret), "RECORDS, TOOK", end.Sub(start))

  start = time.Now()
  session_maps := SessionizeRecords(ret, "session_id")
  end = time.Now()
  fmt.Println("RETURNED", len(session_maps), "SESSIONS, TOOK", end.Sub(start))

  start = time.Now()
  SaveTables()
  end = time.Now()
  fmt.Println("SERIALIZED DB TOOK", end.Sub(start))


}
