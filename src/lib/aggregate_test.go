package sybil_test

import sybil "./"

import "fmt"

import "sort"
import "strconv"
import "math"
import "math/rand"
import "testing"
import "strings"
import "time"

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

	// TEST THAT WE GOT BACK 20 GROUP BY VALUES
	if len(querySpec.Results) != 20 {
		fmt.Println("PIGEON HOLE PRINCIPLED")
	}

	// Test that the group by and int keys are correctly re-assembled
	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GROUP_DELIMITER, "", 1)

		val, err := strconv.ParseInt(k, 10, 64)
		if err != nil || math.Abs(float64(val)-float64(v.Hists["age"].Mean())) > 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, val, v.Hists["age"].Mean())
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

		if math.Abs(float64(avg_age)-float64(v.Hists["age"].Mean())) > 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, avg_age, v.Hists["age"].Mean())
		}
	}
	delete_test_db()

}

// Tests that the histogram works
func TestHistograms(test *testing.T) {
	delete_test_db()

	if testing.Short() {
		test.Skip("Skipping test in short mode")
		return
	}

	block_count := 3

	total_age := int64(0)
	count := 0
	ages := make([]int, 0)

	add_records(func(r *sybil.Record, index int) {
		count++
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		ages = append(ages, int(age))
		total_age += age
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, block_count)

	avg_age := float64(total_age) / float64(count)

	nt := save_and_reload_table(test, block_count)
	var HIST = "hist"
	sybil.FLAGS.OP = &HIST

	querySpec := new_query_spec()
	querySpec.Groups = append(querySpec.Groups, nt.Grouping("age_str"))
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation("age", "hist"))

	nt.MatchAndAggregate(querySpec)

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GROUP_DELIMITER, "", 1)

		kval, _ := strconv.ParseInt(k, 10, 64)
		percentiles := v.Hists["age"].GetPercentiles()
		if int64(percentiles[25]) != kval {
			test.Error("GROUP BY YIELDED UNEXPECTED HIST", k, avg_age, percentiles[25])
		}
		if int64(percentiles[50]) != kval {
			test.Error("GROUP BY YIELDED UNEXPECTED HIST", k, avg_age, percentiles[50])
		}
		if int64(percentiles[75]) != kval {
			test.Error("GROUP BY YIELDED UNEXPECTED HIST", k, avg_age, percentiles[75])
		}
	}

	querySpec = new_query_spec()
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation("age", "hist"))

	nt.MatchAndAggregate(querySpec)

	sort.Ints(ages)

	prev_count := int64(math.MaxInt64)
	// testing that a histogram with single value looks uniform
	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GROUP_DELIMITER, "", 1)
		percentiles := v.Hists["age"].GetPercentiles()

		if v.Count > prev_count {
			test.Error("RESULTS CAME BACK OUT OF COUNT ORDER")
		}

		prev_count = v.Count

		for k, v := range percentiles {
			index := int(float64(k) / 100 * float64(len(ages)))
			val := ages[index]

			// TODO: margin of error should be less than 1!
			if math.Abs(float64(v-int64(val))) > 1 {
				test.Error("P", k, "VAL", v, "EXPECTED", val)
			}
		}

		Debug("PERCENTILES", percentiles)
		Debug("AGES", ages)
		Debug("BUCKETS", v.Hists["age"].GetBuckets())
	}

	querySpec.OrderBy = "age"
	nt.MatchAndAggregate(querySpec)

	sort.Ints(ages)

	prev_avg := float64(0)
	// testing that a histogram with single value looks uniform
	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GROUP_DELIMITER, "", 1)
		avg := v.Hists["age"].Mean()

		if avg < prev_avg {
			test.Error("RESULTS CAME BACK OUT OF COUNT ORDER")
		}

		prev_count = v.Count

	}

	delete_test_db()

}

// Tests that the histogram works
func TestTimeSeries(test *testing.T) {
	delete_test_db()

	if testing.Short() {
		test.Skip("Skipping test in short mode")
		return
	}

	block_count := 3

	total_age := int64(0)
	count := 0
	ages := make([]int, 0)

	add_records(func(r *sybil.Record, index int) {
		count++
		r.AddIntField("id", int64(index))
		random := rand.Intn(50) * -1
		duration := time.Hour * time.Duration(random)
		td := time.Now().Add(duration).Second()
		r.AddIntField("time", int64(td))
		age := int64(rand.Intn(20)) + 10
		ages = append(ages, int(age))
		total_age += age
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, block_count)

	avg_age := float64(total_age) / float64(count)

	nt := save_and_reload_table(test, block_count)

	hist := "hist"
	sybil.FLAGS.OP = &hist
	querySpec := new_query_spec()
	querySpec.Groups = append(querySpec.Groups, nt.Grouping("age_str"))
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation("age", "hist"))
	querySpec.TimeBucket = int(time.Duration(60) * time.Minute)

	nt.MatchAndAggregate(querySpec)

	if len(querySpec.TimeResults) <= 0 {
		test.Error("Time Bucketing returned too little results")
	}

	for _, b := range querySpec.TimeResults {
		if len(b) <= 0 {
			test.Error("TIME BUCKET IS INCORRECTLY EMPTY!")
		}

		for k, v := range b {
			k = strings.Replace(k, sybil.GROUP_DELIMITER, "", 1)

			kval, _ := strconv.ParseInt(k, 10, 64)
			percentiles := v.Hists["age"].GetPercentiles()
			if int64(percentiles[25]) != kval {
				test.Error("GROUP BY YIELDED UNEXPECTED HIST", k, avg_age, percentiles[25])
			}
			if int64(percentiles[50]) != kval {
				test.Error("GROUP BY YIELDED UNEXPECTED HIST", k, avg_age, percentiles[50])
			}
			if int64(percentiles[75]) != kval {
				test.Error("GROUP BY YIELDED UNEXPECTED HIST", k, avg_age, percentiles[75])
			}
		}
	}

	delete_test_db()
}

func TestOrderBy(test *testing.T) {
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

		if math.Abs(float64(avg_age)-float64(v.Hists["age"].Mean())) > 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, avg_age, v.Hists["age"].Mean())
		}
	}

	querySpec.OrderBy = "age"
	nt.MatchAndAggregate(querySpec)

	prev_avg := float64(0)
	// testing that a histogram with single value looks uniform

	if len(querySpec.Results) <= 0 {
		test.Error("NO RESULTS RETURNED FOR QUERY!")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, sybil.GROUP_DELIMITER, "", 1)
		avg := v.Hists["age"].Mean()

		if avg < prev_avg {
			test.Error("RESULTS CAME BACK OUT OF COUNT ORDER")
		}

		prev_avg = avg

	}

	delete_test_db()

}
