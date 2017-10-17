package sybil

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/logv/sybil/src/lib/common"
)

func TestSets(test *testing.T) {
	deleteTestDB()
	totalAge := int64(0)

	addRecordsToTestDB(func(r *Record, i int) {}, 0)
	blockCount := 3
	minCount := CHUNK_SIZE * blockCount
	records := addRecordsToTestDB(func(r *Record, i int) {
		setID := []string{strconv.FormatInt(int64(i), 10)}
		r.AddIntField("id_int", int64(i))
		r.AddSetField("id_set", setID)
		r.AddStrField("id_str", strconv.FormatInt(int64(i), 10))
		age := int64(rand.Intn(20)) + int64(minCount)
		totalAge += age
		r.AddIntField("age", age)
		r.AddStrField("ageStr", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	avgAge := float64(totalAge) / float64(len(records))
	common.Debug("AVG AGE", avgAge-float64(minCount))

	nt := saveAndReloadTestTable(test, blockCount)

	for _, b := range nt.BlockList {
		for _, r := range b.RecordList {
			ival, ok := r.GetIntVal("id_int")
			if !ok {
				test.Error("MISSING INT ID")
			}
			setval, ok := r.GetSetVal("id_set")
			if !ok {
				test.Error("MISSING SET ID")
			}
			strval, ok := r.GetStrVal("id_str")
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
				common.Debug("SET AND STR vals misaligned", setval, strval)
			}

		}
	}

	deleteTestDB()

	// Load Some Samples?

}
