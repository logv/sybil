package edb

// ByAge implements sort.Interface for []Person based on
// the Age field.
type SortIntsByVal []SavedIntBucket

func (a SortIntsByVal) Len() int           { return len(a) }
func (a SortIntsByVal) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortIntsByVal) Less(i, j int) bool { return a[i].Value < a[j].Value }

// Before we save the new record list in a table, we tend to sort by time
type RecordList []*Record
type SortRecordsByTime struct {
	RecordList

	ColId int16
}

func (a RecordList) Len() int      { return len(a) }
func (a RecordList) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SortRecordsByTime) Less(i, j int) bool {
	t1 := a.RecordList[i].Ints[a.ColId]
	t2 := a.RecordList[j].Ints[a.ColId]

	return t1 < t2
}

type SavedIntBucket struct {
	Value   int32
	Records []uint32
}

type SavedSetBucket struct {
	Values  []int32
	Records []uint32
}

type SavedStrBucket struct {
	Value   int32
	Records []uint32
}

type SavedColumnInfo struct {
	NumRecords int32
	StrInfo    StrInfoTable
	IntInfo    IntInfoTable
}

type SavedIntColumn struct {
	Name            string
	NameId          int16
	DeltaEncodedIDs bool
	BucketEncoded	  bool
	Bins            []SavedIntBucket
	Values				  []int32
}
type SavedStrColumn struct {
	Name            string
	NameId          int16
	DeltaEncodedIDs bool
	BucketEncoded	  bool
	Bins            []SavedStrBucket
	Values				  []int32
	StringTable     []string
}
type SavedSetColumn struct {
	Name   string
	NameId int16
	Bins   []SavedSetBucket
}
