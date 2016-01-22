package edb

import "fmt"
import "math/rand"
import "sync"
import "time"
import "bytes"
import "os"
import "log"

import "encoding/gob"

import "github.com/Pallinder/go-randomdata"

type Record struct {
  Ints []IntField
  Strs []StrField
  Sets []SetField

  session_id int
  timestamp  int
}

func (r *Record) getStrVal(name string) (int, bool) {
  id := get_string_id(name);

  for i := 0; i < len(r.Strs); i++ {
    if r.Strs[i].Name == id {
      return r.Strs[i].Value, true;
    }
  }

  return 0, false;
}

func (r *Record) getIntVal(name string) (int, bool) {
 
  id := get_string_id(name);

  for i := 0; i < len(r.Ints); i++ {
    if r.Ints[i].Name == id {
      return r.Ints[i].Value, true;
    }
  }

  return 0, false;
}

func (r *Record) getVal(name string) (int, bool) {
  ret, ok := r.getStrVal(name);
  if !ok {
    ret, ok = r.getIntVal(name);
    if !ok {
      // TODO: throw error
      return 0, false
    }
  }

  return ret, true

}

  
var RECORD_LIST = make([]Record, 0)
var DIRTY = false;

// TODO: insert timestamp (or verify it exists)
// TODO: also verify the session_id exists

var record_m= &sync.Mutex{}

func NewRecord(Ints IntArr, Strs StrArr, Sets SetArr) Record {  
  record_m.Lock();
  r := Record{Sets: Sets, Ints: Ints, Strs: Strs}
  RECORD_LIST = append(RECORD_LIST, r)
  DIRTY = true;
  record_m.Unlock();
  return r
}

func NewRandomRecord() Record{
  r := NewRecord(
    IntArr{ NewIntField("age", rand.Intn(50) + 10), NewIntField("time", int(time.Now().Unix())) }, 
    StrArr{ NewStrField("name", randomdata.FirstName(randomdata.RandomGender)),
      NewStrField("friend", randomdata.FirstName(randomdata.RandomGender)),
      NewStrField("enemy", randomdata.FirstName(randomdata.RandomGender)),
      NewStrField("event", randomdata.City()),
      NewStrField("session_id", string(rand.Intn(5000)))}, 
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

func SaveRecords() {

  if (!DIRTY) {
    return;
  }

  var network bytes.Buffer // Stand-in for the network.

  // Create an encoder and send a value.
  enc := gob.NewEncoder(&network)
  err := enc.Encode(RECORD_LIST);

  if err != nil {
    log.Fatal("encode:", err)
  }

  fmt.Println("SERIALIZED INTO BYTES", network.Len(), "BYTES", "( PER RECORD", network.Len() / len(RECORD_LIST), ")");

  w, _ := os.Create("edb.db")
  network.WriteTo(w);

  DIRTY = false;


}

func LoadRecords() []Record {
  file, _ := os.Open("edb.db")
  // TODO: LOAD FROM FILE
  dec := gob.NewDecoder(file)
  err := dec.Decode(&RECORD_LIST);
  if err != nil {
    fmt.Println("DECODE:", err);
  }

  return RECORD_LIST;

}
