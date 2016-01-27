package edb

import "fmt"
import "sync"

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

func Start() {
  ParseCmdLine()
}
