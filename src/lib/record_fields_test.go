package sybil

import "testing"
import "math/rand"
import "strconv"

func TestSets(t *testing.T) {
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)
	totalAge := int64(0)

	addRecords(tableName, func(r *Record, i int) {}, 0)
	blockCount := 3
	minCount := CHUNK_SIZE * blockCount
	records := addRecords(tableName, func(r *Record, i int) {
		setId := []string{strconv.FormatInt(int64(i), 10), strconv.FormatInt(int64(i), 10)}
		r.AddIntField("id_int", int64(i))
		r.AddSetField("id_set", setId)
		r.AddStrField("id_str", strconv.FormatInt(int64(i), 10))
		age := int64(rand.Intn(20)) + int64(minCount)
		totalAge += age
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	avgAge := float64(totalAge) / float64(len(records))
	Debug("AVG AGE", avgAge-float64(minCount))

	nt := saveAndReloadTable(t, tableName, blockCount)

	for _, b := range nt.BlockList {
		for _, r := range b.RecordList {
			ival, ok := r.GetIntVal("id_int")
			if !ok {
				t.Error("MISSING INT ID")
			}
			setval, ok := r.GetSetVal("id_set")
			if !ok {
				t.Error("MISSING SET ID")
			}
			strval, ok := r.GetStrVal("id_str")
			if !ok {
				t.Error("MISSING STR ID")
			}

			ageval, _ := r.GetStrVal("age_str")
			pval, err := strconv.ParseInt(strval, 10, 64)

			if ageval == strval {
				t.Error("AGE and ID are aligned!", ageval, strval)
			}

			if pval != int64(ival) || err != nil {
				t.Error("STR and INT vals misaligned", ival, strval)
			}

			if strval != setval[0] {
				t.Error("SET AND STR vals misaligned", setval, strval)
			}

		}
	}

	// Load Some Samples?
}
