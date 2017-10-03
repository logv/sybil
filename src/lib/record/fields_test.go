package sybil

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/lib/test_helpers"
)

func TestMain(m *testing.M) {
	RunTests(m)
	DeleteTestDB()
}

func TestSets(test *testing.T) {
	DeleteTestDB()
	totalAge := int64(0)

	AddRecordsToTestDB(func(r *Record, i int) {}, 0)
	blockCount := 3
	minCount := CHUNK_SIZE * blockCount
	records := AddRecordsToTestDB(func(r *Record, i int) {
		setID := []string{strconv.FormatInt(int64(i), 10)}
		AddIntField(r, "id_int", int64(i))
		AddSetField(r, "id_set", setID)
		AddStrField(r, "id_str", strconv.FormatInt(int64(i), 10))
		age := int64(rand.Intn(20)) + int64(minCount)
		totalAge += age
		AddIntField(r, "age", age)
		AddStrField(r, "ageStr", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	avgAge := float64(totalAge) / float64(len(records))
	common.Debug("AVG AGE", avgAge-float64(minCount))

	nt := SaveAndReloadTestTable(test, blockCount)

	for _, b := range nt.BlockList {
		for _, r := range b.RecordList {
			ival, ok := GetIntVal(r, "id_int")
			if !ok {
				test.Error("MISSING INT ID")
			}
			setval, ok := GetSetVal(r, "id_set")
			if !ok {
				test.Error("MISSING SET ID")
			}
			strval, ok := GetStrVal(r, "id_str")
			if !ok {
				test.Error("MISSING STR ID")
			}

			ageval, _ := GetStrVal(r, "ageStr")
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

	DeleteTestDB()

	// Load Some Samples?

}
