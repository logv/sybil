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
  BlockList map[string]TableBlock

  m *sync.Mutex
}

type Result struct {
  Ints map[string]float64
  Strs map[string]string
  Sets map[string][]string
  Hists map[string]*Hist
}

func punctuateSpec(querySpec *QuerySpec) {
  querySpec.Results = make(map[string]*Result)
  querySpec.m = &sync.Mutex{}
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
      buffer.WriteString("total")
    }


    // BUILD GROUPING KEY
    for _, g := range querySpec.Groups {
      col_id := r.block.get_key_id(g.name)
      col := r.block.getColumnInfo(col_id)
      val := col.get_string_for_val(int32(r.Strs[col_id]))
      buffer.WriteString(string(val))
      buffer.WriteString(":")
    }
    group_key := buffer.String()
    buffer.Reset()

    added_record, ok := querySpec.Results[group_key]

    // BUILD GROUPING RECORD
    if !ok {
      querySpec.m.Lock()
      // TODO: make the LIMIT be more intelligent
      length := len(querySpec.Results)
      querySpec.m.Unlock()

      if length >= 1000  {
        continue
      }

      added_record = &Result{ }
      added_record.Hists = make(map[string]*Hist)
      added_record.Ints = make(map[string]float64)
      added_record.Strs = make(map[string]string)
      added_record.Sets = make(map[string][]string)

      querySpec.m.Lock()
      length = len(querySpec.Results)
      if length < 1000 { 
	querySpec.Results[group_key] = added_record
      }
      querySpec.m.Unlock()
    }

    // GO THROUGH AGGREGATIONS AND REALIZE THEM
    querySpec.m.Lock()
    added_record.Ints["c"]++
    count := added_record.Ints["c"]
    querySpec.m.Unlock()

    for _, a := range querySpec.Aggregations {
      val, ok := r.getIntVal(a.name)
      if ok {

        if a.op == "avg" {
          // Calculating averages
          partial, ok := added_record.Ints[a.name]
          if !ok {
            partial = 0
          }

          partial = partial + (float64(val) - partial) / count

          querySpec.m.Lock()
          added_record.Ints[a.name] = partial
          querySpec.m.Unlock()
        }

        if a.op == "hist" {
          hist, ok := added_record.Hists[a.name]
          if !ok { 
            a_id := r.block.get_key_id(a.name)
            hist = r.block.table.NewHist(r.block.table.int_info_table[a_id]) 
            querySpec.m.Lock()
            added_record.Hists[a.name] = hist
            querySpec.m.Unlock()
          }
          hist.addValue(val)
        }
      }

    }

  }


  return ret;
}


func (t *Table) MatchRecords(filters []Filter) []*Record {
  groupings := []Grouping{}
  aggs := []Aggregation {}

  querySpec := QuerySpec{Groups: groupings, Filters: filters, Aggregations: aggs }
  punctuateSpec(&querySpec)

  var wg sync.WaitGroup
  m := &sync.Mutex{}
  rets := make([]*Record, 0)
  count := len(t.newRecords)
  for _, block := range t.BlockList {
    wg.Add(1)
    this_block := block
    go func() {
      defer wg.Done()
      ret := filterAndAggRecords(querySpec, this_block.RecordList[:])
      count += len(ret)

      m.Lock()
      rets = append(rets, ret...)
      m.Unlock()
    }()
  }

  wg.Wait()

  ret := filterAndAggRecords(querySpec, t.newRecords[:])
  rets = append(rets, ret...)

  fmt.Println("FOUND RECORDS", len(rets))

  return rets
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
      records := filterAndAggRecords(querySpec, records[h:e])
      m.Lock()
      ret = append(ret, records...)
      m.Unlock()
    }()

  }

  wg.Wait()

  last_records := records[chunks * chunk_size:]
  records = filterAndAggRecords(querySpec, last_records)
  ret = append(ret, records...)

  return ret
}

func (t *Table) AggRecords(records []*Record, querySpec QuerySpec) {
  start := time.Now()
  MatchAndAggregate(querySpec, records[:])
  end := time.Now()
  fmt.Println("AGGREGATED", len(records), " RECORDS INTO", len(querySpec.Results), "ROLLUPS, TOOK", end.Sub(start))
}

// Aggregations
// Group By

// Aggregations.add(Avg("age"))
// Aggregations.add(Percentile("age", 75))
//
// GroupBy.add("session_id")
