package edb

import "fmt"
import "sync"
import "time"

func make_records() {
  for i := 0; i < 1000; i++ {
    NewRandomRecord(); 
  }
}

func Start() {
  fmt.Println("Starting DB");
  start := time.Now()

  var wg sync.WaitGroup
  for j := 0; j < 100; j++ {
    wg.Add(1);
    go func() {
      make_records()
      defer wg.Done()
    }();
  }

  wg.Wait()
  end := time.Now()
  fmt.Println("CREATED RECORDS, TOOK", end.Sub(start));


  start = time.Now()
  filters := []Filter{NoFilter{}}
  ret := MatchRecords(filters);
  end = time.Now()
  fmt.Println("RETURNED", len(ret), "RECORDS, TOOK", end.Sub(start));


}
