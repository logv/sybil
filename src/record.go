package edb

import "fmt"
import "math/rand"
import "sync"
import "time"

import "github.com/Pallinder/go-randomdata"

type Record struct {
  ints []IntField
  strs []StrField
  sets []SetField

  session_id int
  timestamp  int
}

var RECORD_LIST = make([]Record, 0)

// TODO: insert timestamp (or verify it exists)
// TODO: also verify the session_id exists

var record_m= &sync.Mutex{}

func NewRecord(ints IntArr, strs StrArr, sets SetArr) Record {  
  record_m.Lock();
  r := Record{sets: sets, ints: ints, strs: strs}
  RECORD_LIST = append(RECORD_LIST, r)
  record_m.Unlock();
  return r
}

func NewRandomRecord() Record{
  
  r := NewRecord(
    IntArr{ NewIntField("age", rand.Intn(50) + 10), NewIntField("time", int(time.Now().Unix())) }, 
    StrArr{ NewStrField("name", randomdata.FirstName(randomdata.RandomGender)),
      NewStrField("friend", randomdata.FirstName(randomdata.RandomGender)) }, 
    SetArr{});

  return r;

}

func MatchRecords(filters []Filter) []*Record {
  ret := make([]*Record, 0);

  for i := 0; i < len(RECORD_LIST); i++ {
    add := true;
    r := RECORD_LIST[i];

    for j := 0; j < len(filters); j++ { 
      if filters[j].Filter(r) {
        add = false;
        break;
      }
    }

    if add {
      ret = append(ret, &r);
    }
  }

  return ret;
}


func PrintRecords() {
  for i := 0; i < len(RECORD_LIST); i++ {
    fmt.Println("RECORD", RECORD_LIST[i])
  }
}
