package structs

type RecordList []*Record

func (a RecordList) Len() int      { return len(a) }
func (a RecordList) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

type Record struct {
	Strs      []StrField
	Ints      []IntField
	SetMap    map[int16]SetField
	Populated []int8

	Timestamp int64
	Path      string

	Block *TableBlock
}

var (
	NO_VAL  = int8(0)
	INT_VAL = int8(1)
	STR_VAL = int8(2)
	SET_VAL = int8(3)
)

func ResizeFields(r *Record, length int16) {
	// dont get fooled by zeroes
	if length <= 1 {
		length = 5
	}

	length++

	if int(length) >= len(r.Strs) {
		delta_records := make([]StrField, int(float64(length)))

		r.Strs = append(r.Strs, delta_records...)
	}

	if int(length) >= len(r.Populated) {
		delta_records := make([]int8, int(float64(length)))

		r.Populated = append(r.Populated, delta_records...)
	}

	if int(length) >= len(r.Ints) {
		delta_records := make([]IntField, int(float64(length)))

		r.Ints = append(r.Ints, delta_records...)
	}

}
