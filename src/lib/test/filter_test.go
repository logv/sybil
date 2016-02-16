package sybil_test

import sybil "../"

import "testing"
import "fmt"
import "math/rand"
import "strconv"
import "math"
import "strings"

func TestFilters(test *testing.T) {
	delete_test_db()

	block_count := 3
	add_records(func(r *sybil.Record, i int) {
		age := int64(rand.Intn(20)) + 10

		r.AddIntField("id", int64(i))
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))

	}, block_count)

	save_and_reload_table(test, block_count)

	testIntEq(test)
	testStrEq(test)

	delete_test_db()

}

func testIntEq(test *testing.T) {
	nt := sybil.GetTable(TEST_TABLE_NAME)
	filters := []sybil.Filter{}
	filters = append(filters, nt.IntFilter("age", "eq", 20))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	querySpec := sybil.QuerySpec{Filters: filters, Aggregations: aggs}
	querySpec.Punctuate()

	nt.MatchAndAggregate(&querySpec)

	// Test Filtering to 20..
	if len(querySpec.Results) <= 0 {
		test.Error("Int Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GROUP_DELIMITER, "", 1)

		if math.Abs(20-float64(v.Hists["age"].Avg)) > 0.1 {
			fmt.Println("ACC", v.Hists["age"].Avg)
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Avg)
		}
	}
}

func testStrEq(test *testing.T) {
	nt := sybil.GetTable(TEST_TABLE_NAME)
	filters := []sybil.Filter{}
	filters = append(filters, nt.StrFilter("age_str", "re", "20"))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	querySpec := sybil.QuerySpec{Filters: filters, Aggregations: aggs}
	querySpec.Punctuate()

	nt.MatchAndAggregate(&querySpec)

	if len(querySpec.Results) <= 0 {
		test.Error("Str Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GROUP_DELIMITER, "", 1)

		if math.Abs(20-float64(v.Hists["age"].Avg)) > 0.1 {
			fmt.Println("ACC", v.Hists["age"].Avg)
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Avg)
		}
	}

}
