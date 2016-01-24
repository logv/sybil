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
