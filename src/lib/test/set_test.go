package pcs_test

import pcs "../"

import "testing"
import "math/rand"
import "strconv"
import "log"

func TestSets(test *testing.T) {
	delete_test_db()
	pcs.CHUNK_SIZE = 100

	if testing.Short() {
		test.Skip("Skipping test in short mode")
		return
	}

	BLOCK_COUNT := 3
	COUNT := pcs.CHUNK_SIZE * BLOCK_COUNT
	t := pcs.GetTable(TEST_TABLE_NAME)

	total_age := int64(0)
	for i := 0; i < COUNT; i++ {
		r := t.NewRecord()
		set_id := []string{strconv.FormatInt(int64(i), 10)}
		r.AddIntField("id_int", int64(i))
		r.AddSetField("id_set", set_id)
		r.AddStrField("id_str", strconv.FormatInt(int64(i), 10))
		age := int64(rand.Intn(20)) + 10
		total_age += age
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}

	avg_age := float64(total_age) / float64(COUNT)
	log.Println("AVG AGE", avg_age)

	t.SaveRecords()
	unload_test_table()

	nt := pcs.GetTable(TEST_TABLE_NAME)
	loadSpec := pcs.NewLoadSpec()
	loadSpec.LoadAllColumns = true

	filters := []pcs.Filter{}
	filters = append(filters, nt.IntFilter("age", "eq", 20))

	aggs := []pcs.Aggregation{}
	groupings := []pcs.Grouping{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	querySpec := pcs.QuerySpec{Groups: groupings, Filters: filters, Aggregations: aggs}
	querySpec.Punctuate()

	nt.LoadRecords(&loadSpec)

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
				test.Error("AGE and ID are aligned!")
			}

			if pval != int64(ival) || err != nil {
				test.Error("STR and INT vals misaligned", ival, strval)
			}

			if strval != setval[0] {
				log.Println("SET AND STR vals misaligned", setval, strval)
			}

		}
	}

	delete_test_db()

	// Load Some Samples?

}
