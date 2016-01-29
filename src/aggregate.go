package edb

import "sync"
import "bytes"
import "fmt"
import "time"

func filterAndAggRecords(querySpec QuerySpec, records []*Record) []*Record {
  var buffer bytes.Buffer
  ret := make([]*Record, 0);
  for i := 0; i < len(records); i++ {
    add := true;
    r := records[i];

    // FILTERING
    for j := 0; j < len(querySpec.Filters); j++ {
      if querySpec.Filters[j].Filter(r) {
        add = false;
        break;
      }
    }

    if add {
      ret = append(ret, r);
    }



    // BELOW HERE IS THE AGGREGATION CORE
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
    querySpec.c.Lock()
    added_record.Ints["c"]++
    count := added_record.Ints["c"]
    querySpec.c.Unlock()

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
            hist = r.block.table.NewHist(r.block.table.get_int_info(a_id)) 
            querySpec.m.Lock()
            added_record.Hists[a.name] = hist
            querySpec.m.Unlock()
          }
          hist.addValue(val)
        }
      }

    }

  }


  return ret[:]
}


func (t *Table) MatchAndAggregate(querySpec QuerySpec) {
  start := time.Now()

  var wg sync.WaitGroup
  rets := make([]*Record, 0);

  m := &sync.Mutex{}

  count := 0
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
  end := time.Now()

  querySpec.Matched = rets

  fmt.Println("FILTRD", len(rets), "AND AGGREGATED", "RECORDS INTO", len(querySpec.Results), "RESULTS, TOOK", end.Sub(start))

}
