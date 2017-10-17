package sybil_test

import sybil "./"

import "testing"
import "math/rand"
import "strconv"
import "math"
import "strings"

func TestFilters(test *testing.T) {
	deleteTestDb()

	blockCount := 3
	addRecords(func(r *sybil.Record, i int) {
		age := int64(rand.Intn(20)) + 10

		ageStr := strconv.FormatInt(int64(age), 10)
		r.AddIntField("id", int64(i))
		r.AddIntField("age", age)
		r.AddStrField("ageStr", ageStr)
		r.AddSetField("ageSet", []string{ageStr})

	}, blockCount)

	saveAndReloadTable(test, blockCount)

	sybil.DeleteBlocksAfterQuery = false

	testIntEq(test)
	testIntNeq(test)
	testIntLt(test)
	testIntGt(test)
	testStrEq(test)
	testStrRe(test)
	testStrNeq(test)
	testSetIn(test)
	testSetNin(test)

	deleteTestDb()

}

func testIntLt(test *testing.T) {
	nt := sybil.GetTable(TestTableName)
	filters := []sybil.Filter{}
	filters = append(filters, nt.IntFilter("age", "lt", 20))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	querySpec := sybil.QuerySpec{QueryParams: sybil.QueryParams{Filters: filters, Aggregations: aggs}}

	nt.MatchAndAggregate(&querySpec)

	// Test Filtering to 20..
	if len(querySpec.Results) <= 0 {
		test.Error("Int Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GroupDelimiter, "", 1)

		if math.Abs(float64(v.Hists["age"].Mean())) > 20 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}
}

func testIntGt(test *testing.T) {
	nt := sybil.GetTable(TestTableName)
	filters := []sybil.Filter{}
	filters = append(filters, nt.IntFilter("age", "gt", 20))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	querySpec := sybil.QuerySpec{QueryParams: sybil.QueryParams{Filters: filters, Aggregations: aggs}}

	nt.MatchAndAggregate(&querySpec)

	// Test Filtering to 20..
	if len(querySpec.Results) <= 0 {
		test.Error("Int Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GroupDelimiter, "", 1)

		if math.Abs(float64(v.Hists["age"].Mean())) < 20 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}
}

func testIntNeq(test *testing.T) {
	nt := sybil.GetTable(TestTableName)
	filters := []sybil.Filter{}
	filters = append(filters, nt.IntFilter("age", "neq", 20))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	groupings := []sybil.Grouping{}
	groupings = append(groupings, nt.Grouping("age"))

	querySpec := sybil.QuerySpec{QueryParams: sybil.QueryParams{Filters: filters, Aggregations: aggs, Groups: groupings}}

	nt.MatchAndAggregate(&querySpec)

	// Test Filtering to !20 returns only 19 results (because we have rand(20) above)
	if len(querySpec.Results) != 19 {
		test.Error("Int Filter for age != 20 returned no results")
	}

	if len(querySpec.Results) <= 0 {
		test.Error("Int Filter for age != 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GroupDelimiter, "", 1)

		Debug("TEST INT NEQ", k, v.Hists["age"].Mean())
		if math.Abs(20-float64(v.Hists["age"].Mean())) < 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}
}

func testIntEq(test *testing.T) {
	nt := sybil.GetTable(TestTableName)
	filters := []sybil.Filter{}
	filters = append(filters, nt.IntFilter("age", "eq", 20))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	querySpec := sybil.QuerySpec{QueryParams: sybil.QueryParams{Filters: filters, Aggregations: aggs}}

	nt.MatchAndAggregate(&querySpec)

	// Test Filtering to 20..
	if len(querySpec.Results) <= 0 {
		test.Error("Int Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GroupDelimiter, "", 1)

		if math.Abs(20-float64(v.Hists["age"].Mean())) > 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}
}

func testStrEq(test *testing.T) {
	nt := sybil.GetTable(TestTableName)
	filters := []sybil.Filter{}
	filters = append(filters, nt.StrFilter("ageStr", "re", "20"))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	groupings := []sybil.Grouping{}
	groupings = append(groupings, nt.Grouping("age"))

	querySpec := sybil.QuerySpec{QueryParams: sybil.QueryParams{Filters: filters, Aggregations: aggs, Groups: groupings}}

	Debug("QUERY SPEC", querySpec.Results)

	nt.MatchAndAggregate(&querySpec)
	Debug("QUERY SPEC RESULTS", querySpec.Results)

	if len(querySpec.Results) <= 0 {
		test.Error("Str Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GroupDelimiter, "", 1)

		if math.Abs(20-float64(v.Hists["age"].Mean())) > 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}
}

func testStrNeq(test *testing.T) {
	nt := sybil.GetTable(TestTableName)
	filters := []sybil.Filter{}
	filters = append(filters, nt.StrFilter("ageStr", "nre", "20"))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	groupings := []sybil.Grouping{}
	groupings = append(groupings, nt.Grouping("age"))

	querySpec := sybil.QuerySpec{QueryParams: sybil.QueryParams{Filters: filters, Aggregations: aggs}}

	nt.MatchAndAggregate(&querySpec)

	if len(querySpec.Results) <= 0 {
		test.Error("Str Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GroupDelimiter, "", 1)

		if math.Abs(20-float64(v.Hists["age"].Mean())) < 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}

}

func testStrRe(test *testing.T) {
	nt := sybil.GetTable(TestTableName)
	filters := []sybil.Filter{}
	filters = append(filters, nt.StrFilter("ageStr", "re", "^2"))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	groupings := []sybil.Grouping{}
	groupings = append(groupings, nt.Grouping("age"))

	querySpec := sybil.QuerySpec{QueryParams: sybil.QueryParams{Filters: filters, Aggregations: aggs, Groups: groupings}}

	nt.MatchAndAggregate(&querySpec)

	if len(querySpec.Results) != 10 {
		test.Error("Str Filter for re returned no results", len(querySpec.Results), querySpec.Results)
	}
	if len(querySpec.Results) <= 0 {
		test.Error("Str Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GroupDelimiter, "", 1)

		if v.Hists["age"].Mean()-20 < 0 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}
}

func testSetIn(test *testing.T) {
	nt := sybil.GetTable(TestTableName)
	filters := []sybil.Filter{}
	filters = append(filters, nt.SetFilter("ageSet", "in", "20"))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	groupings := []sybil.Grouping{}
	groupings = append(groupings, nt.Grouping("age"))

	querySpec := sybil.QuerySpec{QueryParams: sybil.QueryParams{Filters: filters, Aggregations: aggs, Groups: groupings}}

	nt.MatchAndAggregate(&querySpec)

	if len(querySpec.Results) != 1 {
		test.Error("Set Filter for in returned more (or less) than one results", len(querySpec.Results), querySpec.Results)
	}

	if len(querySpec.Results) <= 0 {
		test.Error("Set Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GroupDelimiter, "", 1)

		if v.Hists["age"].Mean()-20 < 0 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}

	// TODO: MULTIPLE SET VALUE FILTER
	//	filters = []sybil.Filter{}
	//	filters = append(filters, nt.SetFilter("ageSet", "in", "20,21,22"))
	//	querySpec = sybil.QuerySpec{QueryParams: sybil.QueryParams{Filters: filters, Aggregations: aggs, Groups: groupings}}
	//
	//	if len(querySpec.Results) != 3 {
	//		test.Error("Set Filter for nin returned more (or less) than three results", len(querySpec.Results), querySpec.Results)
	//	}

}

func testSetNin(test *testing.T) {
	nt := sybil.GetTable(TestTableName)
	filters := []sybil.Filter{}
	filters = append(filters, nt.SetFilter("ageSet", "nin", "20"))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "avg"))

	groupings := []sybil.Grouping{}
	groupings = append(groupings, nt.Grouping("age"))

	querySpec := sybil.QuerySpec{QueryParams: sybil.QueryParams{Filters: filters, Aggregations: aggs, Groups: groupings}}

	nt.MatchAndAggregate(&querySpec)

	if len(querySpec.Results) != 19 {
		test.Error("Set Filter for in returned more (or less) than 19 results", len(querySpec.Results), querySpec.Results)
	}

	if len(querySpec.Results) <= 0 {
		test.Error("Set Filter for age 20 returned no results")
	}

}
