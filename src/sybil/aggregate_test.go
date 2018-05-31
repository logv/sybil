package sybil

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestTableLoadRecords(t *testing.T) {
	t.Parallel()
	flags := DefaultFlags()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	//defer deleteTestDb(tableName)

	if testing.Short() {
		t.Skip("Skipping test in short mode")
		return
	}

	blockCount := 3

	addRecords(*flags.DIR, tableName, func(r *Record, index int) {
		r.AddIntField("id", int64(index), *flags.SKIP_OUTLIERS)
		age := int64(rand.Intn(20)) + 10
		r.AddIntField("age", age, *flags.SKIP_OUTLIERS)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	nt := saveAndReloadTable(t, flags, tableName, blockCount)

	querySpec := newQuerySpec()

	querySpec.Groups = append(querySpec.Groups, nt.Grouping("age_str"))
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation(HistogramTypeBasic, "age", "avg"))

	nt.MatchAndAggregate(HistogramParameters{}, querySpec)

	// TEST THAT WE GOT BACK 20 GROUP BY VALUES
	if len(querySpec.Results) != 20 {
		fmt.Println("PIGEON HOLE PRINCIPLED")
	}

	// Test that the group by and int keys are correctly re-assembled
	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		val, err := strconv.ParseInt(k, 10, 64)
		if err != nil || math.Abs(float64(val)-float64(v.Hists["age"].Mean())) > 0.1 {
			t.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, val, v.Hists["age"].Mean())
		}
	}

}

// Tests that the average histogram works
func TestAveraging(t *testing.T) {
	t.Parallel()
	flags := DefaultFlags()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)

	if testing.Short() {
		t.Skip("Skipping test in short mode")
		return
	}

	blockCount := 3

	totalAge := int64(0)
	count := 0
	addRecords(*flags.DIR, tableName, func(r *Record, index int) {
		count++
		r.AddIntField("id", int64(index), *flags.SKIP_OUTLIERS)
		age := int64(rand.Intn(20)) + 10
		totalAge += age
		r.AddIntField("age", age, *flags.SKIP_OUTLIERS)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	avgAge := float64(totalAge) / float64(count)

	nt := saveAndReloadTable(t, flags, tableName, blockCount)

	querySpec := newQuerySpec()
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation(HistogramTypeBasic, "age", "avg"))

	nt.MatchAndAggregate(HistogramParameters{}, querySpec)

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		if math.Abs(float64(avgAge)-float64(v.Hists["age"].Mean())) > 0.1 {
			t.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, avgAge, v.Hists["age"].Mean())
		}
	}
	deleteTestDb(tableName)

}

// Tests that the histogram works
func TestHistograms(t *testing.T) {
	t.Parallel()
	flags := DefaultFlags()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)

	if testing.Short() {
		t.Skip("Skipping test in short mode")
		return
	}

	blockCount := 3

	totalAge := int64(0)
	count := 0
	ages := make([]int, 0)

	addRecords(*flags.DIR, tableName, func(r *Record, index int) {
		count++
		r.AddIntField("id", int64(index), *flags.SKIP_OUTLIERS)
		age := int64(rand.Intn(20)) + 10
		ages = append(ages, int(age))
		totalAge += age
		r.AddIntField("age", age, *flags.SKIP_OUTLIERS)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	avgAge := float64(totalAge) / float64(count)

	nt := saveAndReloadTable(t, flags, tableName, blockCount)
	var HIST = "hist"
	flags.OP = &HIST

	querySpec := newQuerySpec()
	querySpec.HistogramParameters.Type = HistogramTypeBasic
	querySpec.Groups = append(querySpec.Groups, nt.Grouping("age_str"))
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation(HistogramTypeBasic, "age", "hist"))

	nt.MatchAndAggregate(HistogramParameters{Type: HistogramTypeBasic}, querySpec)

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		kval, _ := strconv.ParseInt(k, 10, 64)
		percentiles := v.Hists["age"].GetPercentiles()
		if int64(percentiles[25]) != kval {
			t.Error("GROUP BY YIELDED UNEXPECTED HIST", k, avgAge, percentiles[25])
		}
		if int64(percentiles[50]) != kval {
			t.Error("GROUP BY YIELDED UNEXPECTED HIST", k, avgAge, percentiles[50])
		}
		if int64(percentiles[75]) != kval {
			t.Error("GROUP BY YIELDED UNEXPECTED HIST", k, avgAge, percentiles[75])
		}
	}

	querySpec = newQuerySpec()
	querySpec.HistogramParameters.Type = HistogramTypeBasic
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation(HistogramTypeBasic, "age", "hist"))

	nt.MatchAndAggregate(HistogramParameters{Type: HistogramTypeBasic}, querySpec)

	sort.Ints(ages)

	prevCount := int64(math.MaxInt64)
	// testing that a histogram with single value looks uniform
	for _, v := range querySpec.Results {
		percentiles := v.Hists["age"].GetPercentiles()

		if v.Count > prevCount {
			t.Error("RESULTS CAME BACK OUT OF COUNT ORDER")
		}

		prevCount = v.Count

		for k, v := range percentiles {
			index := int(float64(k) / 100 * float64(len(ages)))
			val := ages[index]

			// TODO: margin of error should be less than 1!
			if math.Abs(float64(v-int64(val))) > 1 {
				t.Error("P", k, "VAL", v, "EXPECTED", val)
			}
		}

		Debug("PERCENTILES", percentiles)
		Debug("AGES", ages)
		Debug("BUCKETS", v.Hists["age"].GetStrBuckets())
	}

	querySpec.OrderBy = "age"
	nt.MatchAndAggregate(HistogramParameters{}, querySpec)

	sort.Ints(ages)

	prevAvg := float64(0)
	// testing that a histogram with single value looks uniform
	for _, v := range querySpec.Results {
		avg := v.Hists["age"].Mean()

		if avg < prevAvg {
			t.Error("RESULTS CAME BACK OUT OF COUNT ORDER")
		}
	}
}

// Tests that the histogram works
func TestTimeSeries(t *testing.T) {
	t.Parallel()
	flags := DefaultFlags()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)

	if testing.Short() {
		t.Skip("Skipping test in short mode")
		return
	}

	blockCount := 3

	totalAge := int64(0)
	count := 0
	ages := make([]int, 0)

	addRecords(*flags.DIR, tableName, func(r *Record, index int) {
		count++
		r.AddIntField("id", int64(index), *flags.SKIP_OUTLIERS)
		random := rand.Intn(50) * -1
		duration := time.Hour * time.Duration(random)
		td := time.Now().Add(duration).Second()
		r.AddIntField("time", int64(td), *flags.SKIP_OUTLIERS)
		age := int64(rand.Intn(20)) + 10
		ages = append(ages, int(age))
		totalAge += age
		r.AddIntField("age", age, *flags.SKIP_OUTLIERS)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	avgAge := float64(totalAge) / float64(count)

	nt := saveAndReloadTable(t, flags, tableName, blockCount)

	querySpec := newQuerySpec()
	querySpec.HistogramParameters.Type = HistogramTypeBasic
	querySpec.Groups = append(querySpec.Groups, nt.Grouping("age_str"))
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation(HistogramTypeBasic, "age", "hist"))
	querySpec.TimeColumn = "time"
	querySpec.TimeBucket = int(time.Duration(60) * time.Minute)

	nt.MatchAndAggregate(HistogramParameters{Type: HistogramTypeBasic}, querySpec)

	if len(querySpec.TimeResults) <= 0 {
		t.Error("Time Bucketing returned too little results")
	}

	for _, b := range querySpec.TimeResults {
		if len(b) <= 0 {
			t.Error("TIME BUCKET IS INCORRECTLY EMPTY!")
		}

		for k, v := range b {
			k = strings.Replace(k, GROUP_DELIMITER, "", 1)

			kval, err := strconv.ParseInt(k, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			percentiles := v.Hists["age"].GetPercentiles()
			if int64(percentiles[25]) != kval {
				t.Fatal("GROUP BY YIELDED UNEXPECTED HIST", k, avgAge, percentiles[25])
			}
			if int64(percentiles[50]) != kval {
				t.Error("GROUP BY YIELDED UNEXPECTED HIST", k, avgAge, percentiles[50])
			}
			if int64(percentiles[75]) != kval {
				t.Error("GROUP BY YIELDED UNEXPECTED HIST", k, avgAge, percentiles[75])
			}
		}
	}
}

func TestOrderBy(t *testing.T) {
	t.Parallel()
	flags := DefaultFlags()
	if testing.Short() {
		t.Skip("Skipping test in short mode")
		return
	}

	blockCount := 3

	totalAge := int64(0)
	count := 0
	tableName := getTestTableName(t)
	addRecords(*flags.DIR, tableName, func(r *Record, index int) {
		count++
		r.AddIntField("id", int64(index), *flags.SKIP_OUTLIERS)
		age := int64(rand.Intn(20)) + 10
		totalAge += age
		r.AddIntField("age", age, *flags.SKIP_OUTLIERS)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	avgAge := float64(totalAge) / float64(count)

	nt := saveAndReloadTable(t, flags, tableName, blockCount)

	querySpec := newQuerySpec()
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation(HistogramTypeBasic, "age", "avg"))

	nt.MatchAndAggregate(HistogramParameters{
		Type: HistogramTypeBasic,
	}, querySpec)

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)

		if math.Abs(float64(avgAge)-float64(v.Hists["age"].Mean())) > 0.1 {
			t.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, avgAge, v.Hists["age"].Mean())
		}
	}

	querySpec.OrderBy = "age"
	nt.MatchAndAggregate(HistogramParameters{}, querySpec)

	prevAvg := float64(0)
	// testing that a histogram with single value looks uniform

	if len(querySpec.Results) <= 0 {
		t.Error("NO RESULTS RETURNED FOR QUERY!")
	}

	for _, v := range querySpec.Results {
		avg := v.Hists["age"].Mean()

		if avg < prevAvg {
			t.Error("RESULTS CAME BACK OUT OF COUNT ORDER")
		}

		prevAvg = avg

	}

	deleteTestDb(tableName)

}
