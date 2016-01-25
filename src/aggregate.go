package edb

import "sync"
import "bytes"
import "fmt"
import "time"

type Aggregation struct {
  op string
  name string
}

type Grouping struct {
  name string
}

type QuerySpec struct {
  Groups []Grouping
  Filters []Filter
  Aggregations []Aggregation
  Results map[string]*Result
  Records *[]*Record

  m *sync.Mutex
}

type Result struct {
  Ints map[string]float64
  Strs map[string]string
  Sets map[string][]string
  Hists map[string]Hist
}

func punctuateSpec(querySpec *QuerySpec, records []*Record) {
  querySpec.Results = make(map[string]*Result)
  querySpec.m = &sync.Mutex{}
  querySpec.Records = &records
}

func filterAndAggRecords(querySpec QuerySpec, records []*Record) []*Record {
  var buffer bytes.Buffer
  ret := make([]*Record, 0);
  for i := 0; i < len(records); i++ {
    add := true;
    r := records[i];
    filters := querySpec.Filters

    for j := 0; j < len(filters); j++ {
      if filters[j].Filter(r) {
        add = false;
        break;
      }
    }

    if add {
      
      ret = append(ret, r);

    }



    // BELOW HERE IS THE AGGREGATION MEAT
    // WE ABORT AGGREGATE IF THERE IS NO GROUP BY SPEC
    if len(querySpec.Groups) == 0 {
      continue
    }


    // BUILD GROUPING KEY
    for _, g := range querySpec.Groups {
      col_id := r.table.get_key_id(g.name)
      val := r.table.get_string_for_val(int32(r.Strs[col_id]))
      buffer.WriteString(string(val))
      buffer.WriteString(":")
    }
    group_key := buffer.String()
    buffer.Reset()

    added_record, ok := querySpec.Results[group_key]

    // BUILD GROUPING RECORD
    if !ok {
      added_record = &Result{ }
      added_record.Hists = make(map[string]Hist)
      added_record.Ints = make(map[string]float64)
      added_record.Strs = make(map[string]string)
      added_record.Sets = make(map[string][]string)

      querySpec.m.Lock()
      querySpec.Results[group_key] = added_record
      querySpec.m.Unlock()
    }

    // GO THROUGH AGGREGATIONS AND REALIZE THEM
    count,ok := added_record.Ints["c"]
    if !ok { count = 0 }
    count++
    querySpec.m.Lock()
    added_record.Ints["c"] =  count
    querySpec.m.Unlock()

    for _, a := range querySpec.Aggregations {
      val, ok := r.getIntVal(a.name)
      if ok {
        partial, ok := added_record.Ints[a.name]
        if !ok {
          partial = 0
        }

        partial = partial + (float64(val) - partial) / count

        querySpec.m.Lock()
        added_record.Ints[a.name] = partial
        querySpec.m.Unlock()
      }

    }

  }

  return ret;
}


func (t *Table) MatchRecords(filters []Filter) []*Record {
  groupings := []Grouping{}
  aggs := []Aggregation {}

  querySpec := QuerySpec{Groups: groupings, Filters: filters, Aggregations: aggs }
  punctuateSpec(&querySpec, t.RecordList)
  return MatchAndAggregate(querySpec, t.RecordList[:])
}


func MatchAndAggregate(querySpec QuerySpec, records []*Record) []*Record {
  var wg sync.WaitGroup
  ret := make([]*Record, 0);

  chunks := 5;
  chunk_size := len(records) / chunks
  m := &sync.Mutex{}

  for c := 0; c < chunks; c++ {
    h := c * chunk_size;
    e := (c+1) * chunk_size

    wg.Add(1)
    go func() {
      defer wg.Done()
      defer m.Unlock()
      records := filterAndAggRecords(querySpec, records[h:e])
      m.Lock()
      ret = append(ret, records...)
    }()

  }

  wg.Wait()

  last_records := records[chunks * chunk_size:]
  records = filterAndAggRecords(querySpec, last_records)
  ret = append(ret, records...)

  return ret
}

func (t *Table) AggRecords(records []*Record) {
  groupings := []Grouping{ Grouping{"session_id"} }
  aggs := []Aggregation {Aggregation{ "avg", "age" }}
  filters := []Filter{}

  querySpec := QuerySpec{Groups: groupings, Filters: filters, Aggregations: aggs }
  punctuateSpec(&querySpec, records)

  start := time.Now()
  MatchAndAggregate(querySpec, records[:])
  end := time.Now()
  fmt.Println("AGGREGATED INTO", len(querySpec.Results), "ROLLUPS, TOOK", end.Sub(start))
}

// Aggregations
// Group By

// Aggregations.add(Avg("age"))
// Aggregations.add(Percentile("age", 75))
//
// GroupBy.add("session_id")
