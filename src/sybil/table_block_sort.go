package sybil

type SortBlocksByTime struct {
	blocks  []*TableBlock
	timeCol string
}

func (a SortBlocksByTime) Len() int      { return len(a.blocks) }
func (a SortBlocksByTime) Swap(i, j int) { a.blocks[i], a.blocks[j] = a.blocks[j], a.blocks[i] }
func (a SortBlocksByTime) Less(i, j int) bool {
	return a.blocks[i].Info.IntInfoMap[a.timeCol].Min < a.blocks[j].Info.IntInfoMap[a.timeCol].Min
}

type SortBlocksByEndTime struct {
	blocks  []*TableBlock
	timeCol string
}

func (a SortBlocksByEndTime) Len() int      { return len(a.blocks) }
func (a SortBlocksByEndTime) Swap(i, j int) { a.blocks[i], a.blocks[j] = a.blocks[j], a.blocks[i] }
func (a SortBlocksByEndTime) Less(i, j int) bool {
	return a.blocks[i].Info.IntInfoMap[a.timeCol].Max < a.blocks[j].Info.IntInfoMap[a.timeCol].Max
}
