package edb

import "bytes"
import "fmt"
import "time"
import "sync"
import "sync/atomic"

var INTERNAL_RESULT_LIMIT = 100000

func filterAndAggRecords(querySpec *QuerySpec, recordsPtr *[]*Record) []*Record {
  var buffer bytes.Buffer
  records := *recordsPtr

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
      col_id := g.name_id
      col := r.block.getColumnInfo(col_id)
      val := col.get_string_for_val(int32(r.Strs[col_id]))
      buffer.WriteString(string(val))
      buffer.WriteString(":")
    }
    group_key := buffer.String()
    buffer.Reset()

    querySpec.r.RLock()
    added_record, ok := querySpec.Results[group_key]
    querySpec.r.RUnlock()

    // BUILD GROUPING RECORD
    if !ok {
      length := len(querySpec.Results)

      if length >= INTERNAL_RESULT_LIMIT  {
        continue
      }

      added_record = &Result{ }
      added_record.Hists = make(map[string]*Hist)
      added_record.Ints = make(map[string]float64)
      added_record.Strs = make(map[string]string)
      added_record.Sets = make(map[string][]string)
      added_record.Count = 0

      // WARNING: this is an annoying thread barrier that happens.
      // TODO: replace it with a RW mutex instead of just R mutex
      querySpec.r.RLock()
      length = len(querySpec.Results)
      existing_record, ok := querySpec.Results[group_key]
      querySpec.r.RUnlock()

      
      if !ok { 
	querySpec.r.Lock()
	querySpec.Results[group_key] = added_record 
	querySpec.r.Unlock()
      } 

      if ok {
	added_record = existing_record
      }
    }

    count := atomic.AddInt32(&added_record.Count, 1)
    // GO THROUGH AGGREGATIONS AND REALIZE THEM

    for _, a := range querySpec.Aggregations {
      a_id := a.name_id
      if r.Populated[a_id] == INT_VAL {
	val := int(r.Ints[a_id])

        if a.op == "avg" {
          // Calculating averages
          partial, ok := added_record.Ints[a.name]
          if !ok {
            partial = 0
          }

          partial = partial + (float64(val) - partial) / float64(count)

          added_record.Ints[a.name] = partial
        }

        if a.op == "hist" {
	  querySpec.m.RLock()
          hist, ok := added_record.Hists[a.name]
	  querySpec.m.RUnlock()

          if !ok { 
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


func (t *Table) MatchAndAggregate(querySpec *QuerySpec) {
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
      ret := filterAndAggRecords(querySpec, &this_block.RecordList)
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
