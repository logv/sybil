package edb

// how do we use hists, anyways?
type Hist struct {
  Max int
  Min int
  Count int
  Avg float64

  bucket_size int
  values map[int]int
  avgs map[int]float64
}

func (t *Table) NewHist(info *IntInfo) *Hist {

  buckets := 200 // resolution?
  h := &Hist{}

  h.values = make(map[int]int, buckets)
  h.avgs = make(map[int]float64, buckets)

  h.Max = int(info.Min)
  h.Min = int(info.Max)
  h.Avg = 0
  h.Count = 0


  size := info.Max - info.Min
  h.bucket_size = size / buckets
  if h.bucket_size == 0 {
    h.bucket_size = 2
  }
  // we should use X buckets to start...
  return h
}

func (h *Hist) addValue(value int) {
  bucket_value := value / h.bucket_size
  partial, ok := h.avgs[bucket_value]
  if !ok {
    partial = 0
  }

  if (value > h.Max) {
    h.Max = value
  } 

  if (value < h.Min) {
    h.Min = value
  }

  h.Count++
  h.Avg = h.Avg + (float64(value) - h.Avg) / float64(h.Count)

  // update counts
  count, ok := h.values[bucket_value]
  if !ok { count = 0 }
  count++
  h.values[bucket_value] = count

  // update bucket averages
  h.avgs[bucket_value] = partial + (float64(value) - partial) / float64(h.values[bucket_value])


}

func (h *Hist) getPercentiles() {

}
