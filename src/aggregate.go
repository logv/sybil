package edb

func AvgRecords(records []*Record, fieldname string) float64 {
  avg := 0.0;
  length := len(records)
  for i := 0; i < length; i++ {
    val, ok :=  records[i].getIntVal(fieldname)
    if ok {
      avg += float64(val) / float64(len(records))
    }
  }

  return float64(avg)
}

// Aggregations
// Group By

// Aggregations.add(Avg("age"))
// Aggregations.add(Percentile("age", 75))
//
// GroupBy.add("session_id")
