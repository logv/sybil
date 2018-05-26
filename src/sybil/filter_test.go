package sybil

import "testing"
import "math/rand"
import "strconv"
import "math"
import "strings"

func TestFilters(t *testing.T) {
	t.Parallel()
	flags := DefaultFlags()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)

	blockCount := 3
	addRecords(tableName, func(r *Record, i int) {
		age := int64(rand.Intn(20)) + 10

		ageStr := strconv.FormatInt(int64(age), 10)
		r.AddIntField(flags, "id", int64(i))
		r.AddIntField(flags, "age", age)
		r.AddStrField("age_str", ageStr)
		r.AddSetField("age_set", []string{ageStr})

	}, blockCount)

	saveAndReloadTable(t, flags, tableName, blockCount)

	testIntEq(t, flags, tableName)
	testIntNeq(t, flags, tableName)
	testIntLt(t, flags, tableName)
	testIntGt(t, flags, tableName)
	testStrEq(t, flags, tableName)
	testStrRe(t, flags, tableName)
	testStrNeq(t, flags, tableName)
	testSetIn(t, flags, tableName)
	testSetNin(t, flags, tableName)

}

func testIntLt(t *testing.T, flags *FlagDefs, tableName string) {
	nt := GetTable(tableName)
	filters := []Filter{}
	filters = append(filters, nt.IntFilter("age", "lt", 20))

	aggs := []Aggregation{}
	aggs = append(aggs, nt.Aggregation(flags, "age", "avg"))

	querySpec := QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs}}

	nt.MatchAndAggregate(flags, &querySpec)

	// Test Filtering to 20..
	if len(querySpec.Results) <= 0 {
		t.Error("Int Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		if math.Abs(float64(v.Hists["age"].Mean())) > 20 {
			t.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}
}

func testIntGt(t *testing.T, flags *FlagDefs, tableName string) {
	nt := GetTable(tableName)
	filters := []Filter{}
	filters = append(filters, nt.IntFilter("age", "gt", 20))

	aggs := []Aggregation{}
	aggs = append(aggs, nt.Aggregation(flags, "age", "avg"))

	querySpec := QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs}}

	nt.MatchAndAggregate(flags, &querySpec)

	// Test Filtering to 20..
	if len(querySpec.Results) <= 0 {
		t.Error("Int Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		if math.Abs(float64(v.Hists["age"].Mean())) < 20 {
			t.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}
}

func testIntNeq(t *testing.T, flags *FlagDefs, tableName string) {
	nt := GetTable(tableName)
	filters := []Filter{}
	filters = append(filters, nt.IntFilter("age", "neq", 20))

	aggs := []Aggregation{}
	aggs = append(aggs, nt.Aggregation(flags, "age", "avg"))

	groupings := []Grouping{}
	groupings = append(groupings, nt.Grouping("age"))

	querySpec := QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs, Groups: groupings}}

	nt.MatchAndAggregate(flags, &querySpec)

	// Test Filtering to !20 returns only 19 results (because we have rand(20) above)
	if len(querySpec.Results) != 19 {
		t.Error("Int Filter for age != 20 returned no results")
	}

	if len(querySpec.Results) <= 0 {
		t.Error("Int Filter for age != 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		Debug("TEST INT NEQ", k, v.Hists["age"].Mean())
		if math.Abs(20-float64(v.Hists["age"].Mean())) < 0.1 {
			t.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}
}

func testIntEq(t *testing.T, flags *FlagDefs, tableName string) {
	nt := GetTable(tableName)
	filters := []Filter{}
	filters = append(filters, nt.IntFilter("age", "eq", 20))

	aggs := []Aggregation{}
	aggs = append(aggs, nt.Aggregation(flags, "age", "avg"))

	querySpec := QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs}}

	nt.MatchAndAggregate(flags, &querySpec)

	// Test Filtering to 20..
	if len(querySpec.Results) <= 0 {
		t.Error("Int Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		if math.Abs(20-float64(v.Hists["age"].Mean())) > 0.1 {
			t.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}
}

func testStrEq(t *testing.T, flags *FlagDefs, tableName string) {
	nt := GetTable(tableName)
	filters := []Filter{}
	filters = append(filters, nt.StrFilter("age_str", "re", "20"))

	aggs := []Aggregation{}
	aggs = append(aggs, nt.Aggregation(flags, "age", "avg"))

	groupings := []Grouping{}
	groupings = append(groupings, nt.Grouping("age"))

	querySpec := QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs, Groups: groupings}}

	Debug("QUERY SPEC", querySpec.Results)

	nt.MatchAndAggregate(flags, &querySpec)
	Debug("QUERY SPEC RESULTS", querySpec.Results)

	if len(querySpec.Results) <= 0 {
		t.Error("Str Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		if math.Abs(20-float64(v.Hists["age"].Mean())) > 0.1 {
			t.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}
}

func testStrNeq(t *testing.T, flags *FlagDefs, tableName string) {
	nt := GetTable(tableName)
	filters := []Filter{}
	filters = append(filters, nt.StrFilter("age_str", "nre", "20"))

	aggs := []Aggregation{}
	aggs = append(aggs, nt.Aggregation(flags, "age", "avg"))

	querySpec := QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs}}

	nt.MatchAndAggregate(flags, &querySpec)

	if len(querySpec.Results) <= 0 {
		t.Error("Str Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		if math.Abs(20-float64(v.Hists["age"].Mean())) < 0.1 {
			t.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}

}

func testStrRe(t *testing.T, flags *FlagDefs, tableName string) {
	nt := GetTable(tableName)
	filters := []Filter{}
	filters = append(filters, nt.StrFilter("age_str", "re", "^2"))

	aggs := []Aggregation{}
	aggs = append(aggs, nt.Aggregation(flags, "age", "avg"))

	groupings := []Grouping{}
	groupings = append(groupings, nt.Grouping("age"))

	querySpec := QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs, Groups: groupings}}

	nt.MatchAndAggregate(flags, &querySpec)

	if len(querySpec.Results) != 10 {
		t.Error("Str Filter for re returned no results", len(querySpec.Results), querySpec.Results)
	}
	if len(querySpec.Results) <= 0 {
		t.Error("Str Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		if v.Hists["age"].Mean()-20 < 0 {
			t.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}
}

func testSetIn(t *testing.T, flags *FlagDefs, tableName string) {
	nt := GetTable(tableName)
	filters := []Filter{}
	filters = append(filters, nt.SetFilter("age_set", "in", "20"))

	aggs := []Aggregation{}
	aggs = append(aggs, nt.Aggregation(flags, "age", "avg"))

	groupings := []Grouping{}
	groupings = append(groupings, nt.Grouping("age"))

	querySpec := QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs, Groups: groupings}}

	nt.MatchAndAggregate(flags, &querySpec)

	if len(querySpec.Results) != 1 {
		t.Error("Set Filter for in returned more (or less) than one results", len(querySpec.Results), querySpec.Results)
	}

	if len(querySpec.Results) <= 0 {
		t.Error("Set Filter for age 20 returned no results")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		if v.Hists["age"].Mean()-20 < 0 {
			t.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, 20, v.Hists["age"].Mean())
		}
	}

	// TODO: MULTIPLE SET VALUE FILTER
	//	filters = []Filter{}
	//	filters = append(filters, nt.SetFilter("age_set", "in", "20,21,22"))
	//	querySpec = QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs, Groups: groupings}}
	//
	//	if len(querySpec.Results) != 3 {
	//		test.Error("Set Filter for nin returned more (or less) than three results", len(querySpec.Results), querySpec.Results)
	//	}

}

func testSetNin(t *testing.T, flags *FlagDefs, tableName string) {
	nt := GetTable(tableName)
	filters := []Filter{}
	filters = append(filters, nt.SetFilter("age_set", "nin", "20"))

	aggs := []Aggregation{}
	aggs = append(aggs, nt.Aggregation(flags, "age", "avg"))

	groupings := []Grouping{}
	groupings = append(groupings, nt.Grouping("age"))

	querySpec := QuerySpec{QueryParams: QueryParams{Filters: filters, Aggregations: aggs, Groups: groupings}}

	nt.MatchAndAggregate(flags, &querySpec)

	if len(querySpec.Results) != 19 {
		t.Error("Set Filter for in returned more (or less) than 19 results", len(querySpec.Results), querySpec.Results)
	}

	if len(querySpec.Results) <= 0 {
		t.Error("Set Filter for age 20 returned no results")
	}

}
