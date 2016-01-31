package edb

import "fmt"
import "time"
import "math"
import "math/rand"
import "strconv"
import "github.com/manveru/faker"

var FAKE, _ = faker.New("en")

func NewRandomRecord(table_name string) *Record {
  t := getTable(table_name)
  r := t.NewRecord()

  if FAKE == nil {
    fmt.Println("NEW FAKER")
    FAKE, _ = faker.New("en")
  }

  r.AddIntField("age", rand.Intn(50) + 10)
  r.AddIntField("f1", rand.Intn(50) + 30)
  r.AddIntField("f2", rand.Intn(50) + 2000000)
  r.AddIntField("f3", rand.Intn(50) * rand.Intn(1000) + 10)
  r.AddIntField("f4", rand.Intn(int(math.Pow(2,30))-1))

  // Make the timestamp + or - 2 weeks
  now := time.Now()
  delta := rand.Intn(int(time.Hour.Nanoseconds()) * 24) * rand.Intn(14)
  if rand.Int() % 2 == 0 { delta = -delta }
  timestamp := now.Add(time.Duration(delta))
  r.AddIntField("time", int(timestamp.Unix()))

  // session is 100,000 cardinality to represent 100,000 individuals
  session_id := int64(rand.Intn(100000))
  r.AddIntField("int_id", int(session_id))
  r.AddStrField("session_id", strconv.FormatInt(session_id, 16))

  canary := int(rand.Intn(10000))
  r.AddIntField("int_canary", canary)
  r.AddStrField("str_canary", strconv.FormatInt(int64(canary), 10))

  // NEED TO LOCK THE FAKER DOWN
  t.record_m.Lock()
  r.AddStrField("state", FAKE.State())
  r.AddStrField("company", FAKE.CompanyName())
  r.AddStrField("country", FAKE.Country())
  r.AddStrField("city", FAKE.City())
  t.record_m.Unlock()

  return r;

}

