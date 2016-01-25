package edb

import "sort"

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
    if (size < 100) {
      h.bucket_size = 1
    } else {
      h.bucket_size = size / 100
    }
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

type ByVal []int
func (a ByVal) Len() int           { return len(a) }
func (a ByVal) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByVal) Less(i, j int) bool { return a[i] < a[j] }


func (h *Hist) getPercentiles() []int {
  percentiles := make([]int, 101)
  keys := make([]int, 0)
  for k,_ := range h.values {
    keys = append(keys, k)
  }
  sort.Sort(ByVal(keys))

  percentiles[0] = h.Min
  count := 0
  prev_p := 0
  for _, k := range keys {
    key_count := h.values[k]
    count = count + key_count
    p := 100 * count / h.Count
    for ip := prev_p; ip < p; ip++ {
      percentiles[ip] = k
    }
    percentiles[p] = k
    prev_p = p
  }



  return percentiles
}
