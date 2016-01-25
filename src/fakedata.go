package edb

import "time"
import "math/rand"
import "strconv"
import "github.com/manveru/faker"
var Faker *faker.Faker


func NewRandomRecord(table_name string) *Record {
  t := getTable(table_name)
  r := t.NewRecord()
  fake, _ := faker.New("en")
  fake.Rand = rand.New(rand.NewSource(42))

  r.AddIntField("age", rand.Intn(50) + 10)
  r.AddIntField("f1", rand.Intn(50) + 30)
  r.AddIntField("f2", rand.Intn(50) + 2000000)
  r.AddIntField("f3", rand.Intn(50) * rand.Intn(1000) + 10)
  r.AddIntField("time", int(time.Now().Unix()))
  r.AddStrField("session_id", strconv.FormatInt(int64(rand.Intn(500000)), 16))
  r.AddStrField("company", fake.CompanyName())
  r.AddStrField("country", fake.Country())
  r.AddStrField("state", fake.State())
  r.AddStrField("city", fake.City())

  return r;

}

