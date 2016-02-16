package sybil_test

import sybil "../"

import "testing"
import "math/rand"
import "strconv"
import "math"
import "strings"
import "log"

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

	sybil.DELETE_BLOCKS_AFTER_QUERY = false

	testIntEq(test)
	testIntNeq(test)
	testIntLt(test)
	testIntGt(test)
	testStrEq(test)
	testStrRe(test)
	testStrNeq(test)

	delete_test_db()

}

func testIntLt(test *testing.T) {
	nt := sybil.GetTable(TEST_TABLE_NAME)
	filters := []sybil.Filter{}
	filters = append(filters, nt.IntFilter("age", "lt", 20))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	querySpec := sybil.QuerySpec{Filters: filters, Aggregations: aggs}

	nt.MatchAndAggregate(&querySpec)

	// Test Filtering to 20..
	if len(querySpec.Results) <= 0 {
		test.Error("Int Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GROUP_DELIMITER, "", 1)

		if math.Abs(float64(v.Hists["age"].Avg)) > 20 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Avg)
		}
	}
}

func testIntGt(test *testing.T) {
	nt := sybil.GetTable(TEST_TABLE_NAME)
	filters := []sybil.Filter{}
	filters = append(filters, nt.IntFilter("age", "gt", 20))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	querySpec := sybil.QuerySpec{Filters: filters, Aggregations: aggs}

	nt.MatchAndAggregate(&querySpec)

	// Test Filtering to 20..
	if len(querySpec.Results) <= 0 {
		test.Error("Int Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GROUP_DELIMITER, "", 1)

		if math.Abs(float64(v.Hists["age"].Avg)) < 20 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Avg)
		}
	}
}

func testIntNeq(test *testing.T) {
	nt := sybil.GetTable(TEST_TABLE_NAME)
	filters := []sybil.Filter{}
	filters = append(filters, nt.IntFilter("age", "neq", 20))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	groupings := []sybil.Grouping{}
	groupings = append(groupings, nt.Grouping("age"))

	querySpec := sybil.QuerySpec{Filters: filters, Aggregations: aggs, Groups: groupings}

	nt.MatchAndAggregate(&querySpec)

	// Test Filtering to !20 returns only 19 results (because we have rand(20) above)
	if len(querySpec.Results) != 19 {
		test.Error("Int Filter for age != 20 returned no results")
	}

	if len(querySpec.Results) <= 0 {
		test.Error("Int Filter for age != 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GROUP_DELIMITER, "", 1)

		log.Println("TEST INT NEQ", k, v.Hists["age"].Avg)
		if math.Abs(20-float64(v.Hists["age"].Avg)) < 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Avg)
		}
	}
}

func testIntEq(test *testing.T) {
	nt := sybil.GetTable(TEST_TABLE_NAME)
	filters := []sybil.Filter{}
	filters = append(filters, nt.IntFilter("age", "eq", 20))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	querySpec := sybil.QuerySpec{Filters: filters, Aggregations: aggs}

	nt.MatchAndAggregate(&querySpec)

	// Test Filtering to 20..
	if len(querySpec.Results) <= 0 {
		test.Error("Int Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GROUP_DELIMITER, "", 1)

		if math.Abs(20-float64(v.Hists["age"].Avg)) > 0.1 {
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

	groupings := []sybil.Grouping{}
	groupings = append(groupings, nt.Grouping("age"))

	querySpec := sybil.QuerySpec{Filters: filters, Aggregations: aggs, Groups: groupings}

	log.Println("QUERY SPEC", querySpec.Results)

	nt.MatchAndAggregate(&querySpec)
	log.Println("QUERY SPEC RESULTS", querySpec.Results)

	if len(querySpec.Results) <= 0 {
		test.Error("Str Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GROUP_DELIMITER, "", 1)

		if math.Abs(20-float64(v.Hists["age"].Avg)) > 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Avg)
		}
	}
}

func testStrNeq(test *testing.T) {
	nt := sybil.GetTable(TEST_TABLE_NAME)
	filters := []sybil.Filter{}
	filters = append(filters, nt.StrFilter("age_str", "nre", "20"))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	groupings := []sybil.Grouping{}
	groupings = append(groupings, nt.Grouping("age"))

	querySpec := sybil.QuerySpec{Filters: filters, Aggregations: aggs}

	nt.MatchAndAggregate(&querySpec)

	if len(querySpec.Results) <= 0 {
		test.Error("Str Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GROUP_DELIMITER, "", 1)

		if math.Abs(20-float64(v.Hists["age"].Avg)) < 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Avg)
		}
	}

}

func testStrRe(test *testing.T) {
	nt := sybil.GetTable(TEST_TABLE_NAME)
	filters := []sybil.Filter{}
	filters = append(filters, nt.StrFilter("age_str", "re", "^2"))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	groupings := []sybil.Grouping{}
	groupings = append(groupings, nt.Grouping("age"))

	querySpec := sybil.QuerySpec{Filters: filters, Aggregations: aggs, Groups: groupings}

	nt.MatchAndAggregate(&querySpec)

	log.Println("THE STR RE", querySpec.Results)

	if len(querySpec.Results) != 10 {
		test.Error("Str Filter for re returned no results", len(querySpec.Results), querySpec.Results)
	}
	if len(querySpec.Results) <= 0 {
		test.Error("Str Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GROUP_DELIMITER, "", 1)

		if v.Hists["age"].Avg-20 < 0 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Avg)
		}
	}

}
