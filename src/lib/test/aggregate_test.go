package pcs_test

import pcs "../"

import "math"
import "fmt"
import "strconv"
import "math/rand"
import "testing"
import "strings"

func TestTableLoadRecords(test *testing.T) {
	delete_test_db()
	pcs.CHUNK_SIZE = 100

	if testing.Short() {
		test.Skip("Skipping test in short mode")
		return
	}

	BLOCK_COUNT := 3
	COUNT := pcs.CHUNK_SIZE * BLOCK_COUNT
	t := pcs.GetTable(TEST_TABLE_NAME)

	for i := 0; i < COUNT; i++ {
		r := t.NewRecord()
		r.AddIntField("id", int64(i))
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}

	t.SaveRecords()

	unload_test_table()

	nt := pcs.GetTable(TEST_TABLE_NAME)
	nt.LoadTableInfo()
	loadSpec := pcs.NewLoadSpec()
	loadSpec.LoadAllColumns = true
	loadSpec.Str("age_str")
	loadSpec.Int("id")
	loadSpec.Int("age")
	count := nt.LoadRecords(&loadSpec)

	if count != COUNT {
		test.Error("Wrote 100 records, but read back", count)
	}

	if len(nt.BlockList) != BLOCK_COUNT {
		test.Error("Wrote", BLOCK_COUNT, "blocks, but came back with", len(nt.BlockList))
	}

	filters := []pcs.Filter{}
	aggs := []pcs.Aggregation{}
	groupings := []pcs.Grouping{}
	groupings = append(groupings, nt.Grouping("age_str"))
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	fmt.Println("GROUPINGS", groupings)

	querySpec := pcs.QuerySpec{Groups: groupings, Filters: filters, Aggregations: aggs}
	querySpec.Punctuate()

	nt.MatchAndAggregate(&querySpec)

	fmt.Println("RESULTS", len(querySpec.Results))

	// TEST THAT WE GOT BACK 20 GROUP BY VALUES
	if len(querySpec.Results) != 20 {
		fmt.Println("PIGEON HOLE PRINCIPLED")
	}

	// Test that the group by and int keys are correctly re-assembled
	for k, v := range querySpec.Results {
		k = strings.Replace(k, pcs.GROUP_DELIMITER, "", 1)

		val, err := strconv.ParseInt(k, 10, 64)
		if err != nil || math.Abs(float64(val)-float64(v.Ints["age"])) > 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, val, v.Ints["age"])
		}
	}

}

func TestAveraging(test *testing.T) {
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
		r.AddIntField("id", int64(i))
		age := int64(rand.Intn(20)) + 10
		total_age += age
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}

	avg_age := float64(total_age) / float64(COUNT)

	t.SaveRecords()

	nt := pcs.GetTable(TEST_TABLE_NAME)
	loadSpec := pcs.NewLoadSpec()
	loadSpec.LoadAllColumns = true
	loadSpec.Str("age_str")
	loadSpec.Int("id")
	loadSpec.Int("age")
	count := nt.LoadRecords(&loadSpec)

	if count != COUNT {
		test.Error("Wrote 100 records, but read back", count)
	}

	if len(nt.BlockList) != BLOCK_COUNT {
		test.Error("Wrote", BLOCK_COUNT, "blocks, but came back with", len(nt.BlockList))
	}

	filters := []pcs.Filter{}
	aggs := []pcs.Aggregation{}
	groupings := []pcs.Grouping{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	fmt.Println("GROUPINGS", groupings)

	querySpec := pcs.QuerySpec{Groups: groupings, Filters: filters, Aggregations: aggs}
	querySpec.Punctuate()

	nt.MatchAndAggregate(&querySpec)

	for k, v := range querySpec.Results {
		k = strings.Replace(k, pcs.GROUP_DELIMITER, "", 1)

		if math.Abs(float64(avg_age)-float64(v.Ints["age"])) > 0.1 {
			fmt.Println("ACC", v.Ints["age"])
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, avg_age, v.Ints["age"])
		}
	}
	fmt.Println("RESULTS", len(querySpec.Results))
	delete_test_db()

}
