package sybil

type SortBlocksByTime []*TableBlock

func (a SortBlocksByTime) Len() int      { return len(a) }
func (a SortBlocksByTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SortBlocksByTime) Less(i, j int) bool {
	timeCol := FLAGS.TIME_COL
	return a[i].Info.IntInfoMap[timeCol].Min < a[j].Info.IntInfoMap[timeCol].Min
}

type SortBlocksByEndTime []*TableBlock

func (a SortBlocksByEndTime) Len() int      { return len(a) }
func (a SortBlocksByEndTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SortBlocksByEndTime) Less(i, j int) bool {
	timeCol := FLAGS.TIME_COL
	return a[i].Info.IntInfoMap[timeCol].Max < a[j].Info.IntInfoMap[timeCol].Max
}
