package edb


// how do we use hists, anyways?
type Hist struct {
  Max int
  Min int
  Avg int

  bucket_size int
  values map[int]int
  avgs map[int]float64
}

func NewHist() Hist {

  return Hist{}
}

func (h *Hist) addValue(value int) {

 
  bucket_value := value / h.bucket_size
  partial, ok := h.avgs[bucket_value]
  if !ok {
    partial = 0
  }

  h.avgs[bucket_value] = partial + (float64(value) - partial) / float64(h.values[bucket_value])
  h.values[bucket_value]++
}

func (h *Hist) getPercentiles() {

}
