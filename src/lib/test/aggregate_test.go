package sybil_test

import sybil "../"

import "math"
import "fmt"
import "strconv"
import "math/rand"
import "testing"
import "strings"

// TESTS:
// table can save and read records in column form
// tests string and ints get re-assembled
func TestTableLoadRecords(test *testing.T) {
	delete_test_db()
	sybil.CHUNK_SIZE = 100

	if testing.Short() {
		test.Skip("Skipping test in short mode")
		return
	}

	sybil.TEST_MODE = true
	BLOCK_COUNT := 3
	COUNT := sybil.CHUNK_SIZE * BLOCK_COUNT
	t := sybil.GetTable(TEST_TABLE_NAME)

	for i := 0; i < COUNT; i++ {
		r := t.NewRecord()
		r.AddIntField("id", int64(i))
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}

	t.SaveRecords()

	unload_test_table()

	nt := sybil.GetTable(TEST_TABLE_NAME)
	nt.LoadTableInfo()
	loadSpec := sybil.NewLoadSpec()
	loadSpec.LoadAllColumns = true
	loadSpec.Str("age_str")
	loadSpec.Int("id")
	loadSpec.Int("age")
	count := nt.LoadRecords(&loadSpec)

	if count != COUNT {
		test.Error("Wrote 100 records, but read back", count)
	}

	// +1 is the Row Store Block...
	if len(nt.BlockList) != BLOCK_COUNT+1 {
		test.Error("Wrote", BLOCK_COUNT, "blocks, but came back with", len(nt.BlockList))
	}

	filters := []sybil.Filter{}
	aggs := []sybil.Aggregation{}
	groupings := []sybil.Grouping{}
	groupings = append(groupings, nt.Grouping("age_str"))
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	fmt.Println("GROUPINGS", groupings)

	querySpec := sybil.QuerySpec{Groups: groupings, Filters: filters, Aggregations: aggs}
	querySpec.Punctuate()

	nt.MatchAndAggregate(&querySpec)

	fmt.Println("RESULTS", len(querySpec.Results))

	// TEST THAT WE GOT BACK 20 GROUP BY VALUES
	if len(querySpec.Results) != 20 {
		fmt.Println("PIGEON HOLE PRINCIPLED")
	}

	// Test that the group by and int keys are correctly re-assembled
	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GROUP_DELIMITER, "", 1)

		val, err := strconv.ParseInt(k, 10, 64)
		if err != nil || math.Abs(float64(val)-float64(v.Hists["age"].Avg)) > 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, val, v.Hists["age"].Avg)
		}
	}

}

// Tests that the average histogram works
func TestAveraging(test *testing.T) {
	delete_test_db()
	sybil.CHUNK_SIZE = 100

	if testing.Short() {
		test.Skip("Skipping test in short mode")
		return
	}

	sybil.TEST_MODE = true

	BLOCK_COUNT := 3
	COUNT := sybil.CHUNK_SIZE * BLOCK_COUNT
	t := sybil.GetTable(TEST_TABLE_NAME)

	total_age := int64(0)
	for i := 0; i < COUNT; i++ {
		r := t.NewRecord()
		r.AddIntField("id", int64(i))
		age := int64(rand.Intn(20)) + 10
		total_age += age
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}

	avg_age := float64(total_age) / float64(COUNT)

	t.SaveRecords()

	nt := sybil.GetTable(TEST_TABLE_NAME)
	loadSpec := sybil.NewLoadSpec()
	loadSpec.LoadAllColumns = true
	loadSpec.Str("age_str")
	loadSpec.Int("id")
	loadSpec.Int("age")
	count := nt.LoadRecords(&loadSpec)

	if count != COUNT {
		test.Error("Wrote", COUNT, "records, but read back", count)
	}

	if len(nt.BlockList) != BLOCK_COUNT+1 {
		test.Error("Wrote", BLOCK_COUNT, "blocks, but came back with", len(nt.BlockList))
	}

	filters := []sybil.Filter{}
	aggs := []sybil.Aggregation{}
	groupings := []sybil.Grouping{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	fmt.Println("GROUPINGS", groupings)

	querySpec := sybil.QuerySpec{Groups: groupings, Filters: filters, Aggregations: aggs}
	querySpec.Punctuate()

	nt.MatchAndAggregate(&querySpec)

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GROUP_DELIMITER, "", 1)

		if math.Abs(float64(avg_age)-float64(v.Hists["age"].Avg)) > 0.1 {
			fmt.Println("ACC", v.Hists["age"].Avg)
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, avg_age, v.Hists["age"].Avg)
		}
	}
	fmt.Println("RESULTS", len(querySpec.Results))
	delete_test_db()

}
