package sybil

import "fmt"

import "sort"
import "strconv"
import "math"
import "math/rand"
import "testing"
import "strings"
import "time"

func TestTableLoadRecords(test *testing.T) {
	deleteTestDb()

	if testing.Short() {
		test.Skip("Skipping test in short mode")
		return
	}

	blockCount := 3

	addRecords(func(r *Record, index int) {
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	nt := saveAndReloadTable(test, blockCount)

	querySpec := newQuerySpec()

	querySpec.Groups = append(querySpec.Groups, nt.Grouping("age_str"))
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation("age", "avg"))

	nt.MatchAndAggregate(querySpec)

	// TEST THAT WE GOT BACK 20 GROUP BY VALUES
	if len(querySpec.Results) != 20 {
		fmt.Println("PIGEON HOLE PRINCIPLED")
	}

	// Test that the group by and int keys are correctly re-assembled
	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		val, err := strconv.ParseInt(k, 10, 64)
		if err != nil || math.Abs(float64(val)-float64(v.Hists["age"].Mean())) > 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, val, v.Hists["age"].Mean())
		}
	}

}

// Tests that the average histogram works
func TestAveraging(test *testing.T) {
	deleteTestDb()

	if testing.Short() {
		test.Skip("Skipping test in short mode")
		return
	}

	blockCount := 3

	totalAge := int64(0)
	count := 0
	addRecords(func(r *Record, index int) {
		count++
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		totalAge += age
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	avgAge := float64(totalAge) / float64(count)

	nt := saveAndReloadTable(test, blockCount)

	querySpec := newQuerySpec()
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation("age", "avg"))

	nt.MatchAndAggregate(querySpec)

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		if math.Abs(float64(avgAge)-float64(v.Hists["age"].Mean())) > 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, avgAge, v.Hists["age"].Mean())
		}
	}
	deleteTestDb()

}

// Tests that the histogram works
func TestHistograms(test *testing.T) {
	deleteTestDb()

	if testing.Short() {
		test.Skip("Skipping test in short mode")
		return
	}

	blockCount := 3

	totalAge := int64(0)
	count := 0
	ages := make([]int, 0)

	addRecords(func(r *Record, index int) {
		count++
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		ages = append(ages, int(age))
		totalAge += age
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	avgAge := float64(totalAge) / float64(count)

	nt := saveAndReloadTable(test, blockCount)
	var HIST = "hist"
	FLAGS.OP = &HIST

	querySpec := newQuerySpec()
	querySpec.Groups = append(querySpec.Groups, nt.Grouping("age_str"))
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation("age", "hist"))

	nt.MatchAndAggregate(querySpec)

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		kval, _ := strconv.ParseInt(k, 10, 64)
		percentiles := v.Hists["age"].GetPercentiles()
		if int64(percentiles[25]) != kval {
			test.Error("GROUP BY YIELDED UNEXPECTED HIST", k, avgAge, percentiles[25])
		}
		if int64(percentiles[50]) != kval {
			test.Error("GROUP BY YIELDED UNEXPECTED HIST", k, avgAge, percentiles[50])
		}
		if int64(percentiles[75]) != kval {
			test.Error("GROUP BY YIELDED UNEXPECTED HIST", k, avgAge, percentiles[75])
		}
	}

	querySpec = newQuerySpec()
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation("age", "hist"))

	nt.MatchAndAggregate(querySpec)

	sort.Ints(ages)

	prevCount := int64(math.MaxInt64)
	// testing that a histogram with single value looks uniform
	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)
		percentiles := v.Hists["age"].GetPercentiles()

		if v.Count > prevCount {
			test.Error("RESULTS CAME BACK OUT OF COUNT ORDER")
		}

		prevCount = v.Count

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
		Debug("BUCKETS", v.Hists["age"].GetStrBuckets())
	}

	querySpec.OrderBy = "age"
	nt.MatchAndAggregate(querySpec)

	sort.Ints(ages)

	prevAvg := float64(0)
	// testing that a histogram with single value looks uniform
	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)
		avg := v.Hists["age"].Mean()

		if avg < prevAvg {
			test.Error("RESULTS CAME BACK OUT OF COUNT ORDER")
		}

		prevCount = v.Count

	}

	deleteTestDb()

}

// Tests that the histogram works
func TestTimeSeries(test *testing.T) {
	deleteTestDb()

	if testing.Short() {
		test.Skip("Skipping test in short mode")
		return
	}

	blockCount := 3

	totalAge := int64(0)
	count := 0
	ages := make([]int, 0)

	addRecords(func(r *Record, index int) {
		count++
		r.AddIntField("id", int64(index))
		random := rand.Intn(50) * -1
		duration := time.Hour * time.Duration(random)
		td := time.Now().Add(duration).Second()
		r.AddIntField("time", int64(td))
		age := int64(rand.Intn(20)) + 10
		ages = append(ages, int(age))
		totalAge += age
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	avgAge := float64(totalAge) / float64(count)

	nt := saveAndReloadTable(test, blockCount)

	hist := "hist"
	FLAGS.OP = &hist
	querySpec := newQuerySpec()
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
			k = strings.Replace(k, GROUP_DELIMITER, "", 1)

			kval, _ := strconv.ParseInt(k, 10, 64)
			percentiles := v.Hists["age"].GetPercentiles()
			if int64(percentiles[25]) != kval {
				test.Error("GROUP BY YIELDED UNEXPECTED HIST", k, avgAge, percentiles[25])
			}
			if int64(percentiles[50]) != kval {
				test.Error("GROUP BY YIELDED UNEXPECTED HIST", k, avgAge, percentiles[50])
			}
			if int64(percentiles[75]) != kval {
				test.Error("GROUP BY YIELDED UNEXPECTED HIST", k, avgAge, percentiles[75])
			}
		}
	}

	deleteTestDb()
}

func TestOrderBy(test *testing.T) {
	if testing.Short() {
		test.Skip("Skipping test in short mode")
		return
	}

	blockCount := 3

	totalAge := int64(0)
	count := 0
	addRecords(func(r *Record, index int) {
		count++
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		totalAge += age
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	avgAge := float64(totalAge) / float64(count)

	nt := saveAndReloadTable(test, blockCount)

	querySpec := newQuerySpec()
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation("age", "avg"))

	nt.MatchAndAggregate(querySpec)

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		if math.Abs(float64(avgAge)-float64(v.Hists["age"].Mean())) > 0.1 {
			test.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, avgAge, v.Hists["age"].Mean())
		}
	}

	querySpec.OrderBy = "age"
	nt.MatchAndAggregate(querySpec)

	prevAvg := float64(0)
	// testing that a histogram with single value looks uniform

	if len(querySpec.Results) <= 0 {
		test.Error("NO RESULTS RETURNED FOR QUERY!")
	}

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)
		avg := v.Hists["age"].Mean()

		if avg < prevAvg {
			test.Error("RESULTS CAME BACK OUT OF COUNT ORDER")
		}

		prevAvg = avg

	}

	deleteTestDb()

}
