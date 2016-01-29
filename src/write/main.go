package main

import "flag"
import "fmt"
import "sync"

import edb "../lib"

var f_ADD_RECORDS = flag.Int("add", 0, "Add data?")
var f_TABLE = flag.String("table", "", "Table to operate on")

func make_records(name string) {
  fmt.Println("Adding", *f_ADD_RECORDS, "to", name)
  CHUNK_SIZE := 50000
  var wg sync.WaitGroup
  for i := 0; i < *f_ADD_RECORDS  / CHUNK_SIZE; i++ {
    wg.Add(1)
    go func() {
      defer wg.Done()
      for j := 0; j < CHUNK_SIZE; j++ {
	edb.NewRandomRecord(name); 
      }
    }()
  }

  for j := 0; j < *f_ADD_RECORDS % CHUNK_SIZE; j++ {
    edb.NewRandomRecord(name); 
  }

  wg.Wait()


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
func main() {
  flag.Parse()
  // add records should happen after we load records
  if (*f_ADD_RECORDS != 0) {	
    add_records()
  }
}
