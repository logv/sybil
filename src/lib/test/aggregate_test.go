package sybil_test

import sybil "../"

import "math"
import "fmt"
import "strconv"
import "math/rand"
import "testing"
import "strings"

func TestTableLoadRecords(test *testing.T) {
	delete_test_db()

	if testing.Short() {
		test.Skip("Skipping test in short mode")
		return
	}

	block_count := 3

	add_records(func(r *sybil.Record, index int) {
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, block_count)

	nt := save_and_reload_table(test, block_count)

	querySpec := new_query_spec()

	querySpec.Groups = append(querySpec.Groups, nt.Grouping("age_str"))
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation("age", "avg"))

	nt.MatchAndAggregate(querySpec)

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

	if testing.Short() {
		test.Skip("Skipping test in short mode")
		return
	}

	block_count := 3

	total_age := int64(0)
	count := 0
	add_records(func(r *sybil.Record, index int) {
		count++
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		total_age += age
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, block_count)

	avg_age := float64(total_age) / float64(count)

	nt := save_and_reload_table(test, block_count)

	querySpec := new_query_spec()
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation("age", "avg"))

	nt.MatchAndAggregate(querySpec)

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
