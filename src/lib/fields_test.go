package sybil_test

import sybil "./"

import "testing"
import "math/rand"
import "strconv"


func TestSets(test *testing.T) {
	delete_test_db()
	total_age := int64(0)

	add_records(func(r *sybil.Record, i int) {}, 0)
	block_count := 3
	min_count := sybil.CHUNK_SIZE * block_count
	records := add_records(func(r *sybil.Record, i int) {
		set_id := []string{strconv.FormatInt(int64(i), 10)}
		r.AddIntField("id_int", int64(i))
		r.AddSetField("id_set", set_id)
		r.AddStrField("id_str", strconv.FormatInt(int64(i), 10))
		age := int64(rand.Intn(20)) + int64(min_count)
		total_age += age
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, block_count)

	avg_age := float64(total_age) / float64(len(records))
	Debug("AVG AGE", avg_age-float64(min_count))

	nt := save_and_reload_table(test, block_count)

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

			ageval, _ := r.GetStrVal("age_str")
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

	delete_test_db()

	// Load Some Samples?

}
