package sybil_test

import sybil "./"

import "testing"
import "math/rand"
import "strconv"

func TestSets(test *testing.T) {
	deleteTestDb()
	totalAge := int64(0)

	addRecords(func(r *sybil.Record, i int) {}, 0)
	blockCount := 3
	minCount := sybil.ChunkSize * blockCount
	records := addRecords(func(r *sybil.Record, i int) {
		setID := []string{strconv.FormatInt(int64(i), 10)}
		r.AddIntField("idInt", int64(i))
		r.AddSetField("idSet", setID)
		r.AddStrField("idStr", strconv.FormatInt(int64(i), 10))
		age := int64(rand.Intn(20)) + int64(minCount)
		totalAge += age
		r.AddIntField("age", age)
		r.AddStrField("ageStr", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	avgAge := float64(totalAge) / float64(len(records))
	Debug("AVG AGE", avgAge-float64(minCount))

	nt := saveAndReloadTable(test, blockCount)

	for _, b := range nt.BlockList {
		for _, r := range b.RecordList {
			ival, ok := r.GetIntVal("idInt")
			if !ok {
				test.Error("MISSING INT ID")
			}
			setval, ok := r.GetSetVal("idSet")
			if !ok {
				test.Error("MISSING SET ID")
			}
			strval, ok := r.GetStrVal("idStr")
			if !ok {
				test.Error("MISSING STR ID")
			}

			ageval, _ := r.GetStrVal("ageStr")
			pval, err := strconv.ParseInt(strval, 10, 64)

			if ageval == strval {
				test.Error("AGE and ID are aligned!", ageval, strval)
			}

			if pval != int64(ival) || err != nil {
				test.Error("STR and INT vals misaligned", ival, strval)
			}

			if strval != setval[0] {
				Debug("SET AND STR vals misaligned", setval, strval)
			}

		}
	}

	deleteTestDb()

	// Load Some Samples?

}
