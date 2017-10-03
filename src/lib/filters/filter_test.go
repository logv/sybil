package sybil

import (
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	. "github.com/logv/sybil/src/lib/aggregate"
	. "github.com/logv/sybil/src/lib/column_store"
	"github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/record"
	. "github.com/logv/sybil/src/lib/specs"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/lib/test_helpers"
)

func TestFilters(test *testing.T) {
	DeleteTestDB()

	blockCount := 3
	AddRecordsToTestDB(func(r *Record, i int) {
		age := int64(rand.Intn(20)) + 10

		ageStr := strconv.FormatInt(int64(age), 10)
		AddIntField(r, "id", int64(i))
		AddIntField(r, "age", age)
		AddStrField(r, "ageStr", ageStr)
		AddSetField(r, "age_set", []string{ageStr})

	}, blockCount)

	SaveAndReloadTestTable(test, blockCount)

	DELETE_BLOCKS_AFTER_QUERY = false

	testIntEq(test)
	testIntNeq(test)
	testIntLt(test)
	testIntGt(test)
	testStrEq(test)
	testStrRe(test)
	testStrNeq(test)
	testSetIn(test)
	testSetNin(test)

	DeleteTestDB()

}

func testIntLt(test *testing.T) {
	nt := GetTable(TEST_TABLE_NAME)
	filters := []Filter{}
	filters = append(filters, MakeIntFilter(nt, "age", "lt", 20))

	aggs := []Aggregation{}
	aggs = append(aggs, AggregationForTable(nt, "age", "avg"))

	querySpec := QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs}}

	MatchAndAggregate(nt, &querySpec)

	// Test Filtering to 20..
	if len(querySpec.Results) <= 0 {
		test.Error("Int Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		if math.Abs(float64(v.Hists["age"].Mean())) > 20 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}
}

func testIntGt(test *testing.T) {
	nt := GetTable(TEST_TABLE_NAME)
	filters := []Filter{}
	filters = append(filters, MakeIntFilter(nt, "age", "gt", 20))

	aggs := []Aggregation{}
	aggs = append(aggs, AggregationForTable(nt, "age", "avg"))

	querySpec := QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs}}

	MatchAndAggregate(nt, &querySpec)

	// Test Filtering to 20..
	if len(querySpec.Results) <= 0 {
		test.Error("Int Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		if math.Abs(float64(v.Hists["age"].Mean())) < 20 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}
}

func testIntNeq(test *testing.T) {
	nt := GetTable(TEST_TABLE_NAME)
	filters := []Filter{}
	filters = append(filters, MakeIntFilter(nt, "age", "neq", 20))

	aggs := []Aggregation{}
	aggs = append(aggs, AggregationForTable(nt, "age", "avg"))

	groupings := []Grouping{}
	groupings = append(groupings, GroupingForTable(nt, "age"))

	querySpec := QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs, Groups: groupings}}

	MatchAndAggregate(nt, &querySpec)

	// Test Filtering to !20 returns only 19 results (because we have rand(20) above)
	if len(querySpec.Results) != 19 {
		test.Error("Int Filter for age != 20 returned no results")
	}

	if len(querySpec.Results) <= 0 {
		test.Error("Int Filter for age != 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		common.Debug("TEST INT NEQ", k, v.Hists["age"].Mean())
		if math.Abs(20-float64(v.Hists["age"].Mean())) < 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}
}

func testIntEq(test *testing.T) {
	nt := GetTable(TEST_TABLE_NAME)
	filters := []Filter{}
	filters = append(filters, MakeIntFilter(nt, "age", "eq", 20))

	aggs := []Aggregation{}
	aggs = append(aggs, AggregationForTable(nt, "age", "avg"))

	querySpec := QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs}}

	MatchAndAggregate(nt, &querySpec)

	// Test Filtering to 20..
	if len(querySpec.Results) <= 0 {
		test.Error("Int Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		if math.Abs(20-float64(v.Hists["age"].Mean())) > 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}
}

func testStrEq(test *testing.T) {
	nt := GetTable(TEST_TABLE_NAME)
	filters := []Filter{}
	filters = append(filters, MakeStrFilter(nt, "ageStr", "re", "20"))

	aggs := []Aggregation{}
	aggs = append(aggs, AggregationForTable(nt, "age", "avg"))

	groupings := []Grouping{}
	groupings = append(groupings, GroupingForTable(nt, "age"))

	querySpec := QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs, Groups: groupings}}

	common.Debug("QUERY SPEC", querySpec.Results)

	MatchAndAggregate(nt, &querySpec)
	common.Debug("QUERY SPEC RESULTS", querySpec.Results)

	if len(querySpec.Results) <= 0 {
		test.Error("Str Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		if math.Abs(20-float64(v.Hists["age"].Mean())) > 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}
}

func testStrNeq(test *testing.T) {
	nt := GetTable(TEST_TABLE_NAME)
	filters := []Filter{}
	filters = append(filters, MakeStrFilter(nt, "ageStr", "nre", "20"))

	aggs := []Aggregation{}
	aggs = append(aggs, AggregationForTable(nt, "age", "avg"))

	groupings := []Grouping{}
	groupings = append(groupings, GroupingForTable(nt, "age"))

	querySpec := QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs}}

	MatchAndAggregate(nt, &querySpec)

	if len(querySpec.Results) <= 0 {
		test.Error("Str Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		if math.Abs(20-float64(v.Hists["age"].Mean())) < 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}

}

func testStrRe(test *testing.T) {
	nt := GetTable(TEST_TABLE_NAME)
	filters := []Filter{}
	filters = append(filters, MakeStrFilter(nt, "ageStr", "re", "^2"))

	aggs := []Aggregation{}
	aggs = append(aggs, AggregationForTable(nt, "age", "avg"))

	groupings := []Grouping{}
	groupings = append(groupings, GroupingForTable(nt, "age"))

	querySpec := QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs, Groups: groupings}}

	MatchAndAggregate(nt, &querySpec)

	if len(querySpec.Results) != 10 {
		test.Error("Str Filter for re returned no results", len(querySpec.Results), querySpec.Results)
	}
	if len(querySpec.Results) <= 0 {
		test.Error("Str Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		if v.Hists["age"].Mean()-20 < 0 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}
}

func testSetIn(test *testing.T) {
	nt := GetTable(TEST_TABLE_NAME)
	filters := []Filter{}
	filters = append(filters, MakeSetFilter(nt, "age_set", "in", "20"))

	aggs := []Aggregation{}
	aggs = append(aggs, AggregationForTable(nt, "age", "avg"))

	groupings := []Grouping{}
	groupings = append(groupings, GroupingForTable(nt, "age"))

	querySpec := QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs, Groups: groupings}}

	MatchAndAggregate(nt, &querySpec)

	if len(querySpec.Results) != 1 {
		test.Error("Set Filter for in returned more (or less) than one results", len(querySpec.Results), querySpec.Results)
	}

	if len(querySpec.Results) <= 0 {
		test.Error("Set Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		if v.Hists["age"].Mean()-20 < 0 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}

	// TODO: MULTIPLE SET VALUE FILTER
	//	filters = []Filter{}
	//	filters = append(filters, MakeSetFilter(nt, "age_set", "in", "20,21,22"))
	//	querySpec = QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs, Groups: groupings}}
	//
	//	if len(querySpec.Results) != 3 {
	//		test.Error("Set Filter for nin returned more (or less) than three results", len(querySpec.Results), querySpec.Results)
	//	}

}

func testSetNin(test *testing.T) {
	nt := GetTable(TEST_TABLE_NAME)
	filters := []Filter{}
	filters = append(filters, MakeSetFilter(nt, "age_set", "nin", "20"))

	aggs := []Aggregation{}
	aggs = append(aggs, AggregationForTable(nt, "age", "avg"))

	groupings := []Grouping{}
	groupings = append(groupings, GroupingForTable(nt, "age"))

	querySpec := QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs, Groups: groupings}}

	MatchAndAggregate(nt, &querySpec)

	if len(querySpec.Results) != 19 {
		test.Error("Set Filter for in returned more (or less) than 19 results", len(querySpec.Results), querySpec.Results)
	}

	if len(querySpec.Results) <= 0 {
		test.Error("Set Filter for age 20 returned no results")
	}

}
