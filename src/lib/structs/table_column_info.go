package structs

import "sort"

var TOP_STRING_COUNT = 20

type StrInfoCol struct {
	Name  int32
	Value int
}

type SortStrsByCount []StrInfoCol

func (a SortStrsByCount) Len() int           { return len(a) }
func (a SortStrsByCount) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortStrsByCount) Less(i, j int) bool { return a[i].Value < a[j].Value }

func (si *StrInfo) Prune() {
	si.Cardinality = len(si.TopStringCount)

	if si.Cardinality > TOP_STRING_COUNT {
		interim := make([]StrInfoCol, 0)

		for s, c := range si.TopStringCount {
			interim = append(interim, StrInfoCol{Name: s, Value: c})
		}

		sort.Sort(SortStrsByCount(interim))

		for _, x := range interim[:len(si.TopStringCount)-TOP_STRING_COUNT-1] {
			delete(si.TopStringCount, x.Name)
		}
	}

}
