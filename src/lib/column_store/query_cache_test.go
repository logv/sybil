package sybil

import (
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/logv/sybil/src/lib/common"
	"github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/filters"
	. "github.com/logv/sybil/src/lib/record"
	. "github.com/logv/sybil/src/lib/specs"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/lib/test_helpers"
)

func TestCachedQueries(test *testing.T) {
	DeleteTestDB()

	blockCount := 5

	DELETE_BLOCKS_AFTER_QUERY = false
	config.FLAGS.CACHED_QUERIES = &config.TRUE

	addRecords := func(blockCount int) {
		AddRecordsToTestDB(func(r *Record, i int) {
			age := int64(rand.Intn(20)) + 10

			ageStr := strconv.FormatInt(int64(age), 10)
			AddIntField(r, "id", int64(i))
			AddIntField(r, "age", age)
			AddStrField(r, "ageStr", ageStr)
			AddSetField(r, "age_set", []string{ageStr})

		}, blockCount)
		SaveAndReloadTestTable(test, blockCount)

	}

	addRecords(blockCount)
	testCachedQueryFiles(test)
	DeleteTestDB()

	addRecords(blockCount)
	testCachedQueryConsistency(test)
	DeleteTestDB()

	addRecords(blockCount)
	testCachedBasicHist(test)
	DeleteTestDB()

	config.FLAGS.CACHED_QUERIES = &config.FALSE

}

func testCachedQueryFiles(test *testing.T) {
	nt := GetTable(TEST_TABLE_NAME)
	filters := []Filter{}
	filters = append(filters, MakeIntFilter(nt, "age", "lt", 20))

	aggs := []Aggregation{}
	aggs = append(aggs, AggregationForTable(nt, "age", "hist"))

	querySpec := QuerySpec{Table: nt,
		QueryParams: QueryParams{Filters: filters, Aggregations: aggs}}
	loadSpec := NewLoadSpec()
	loadSpec.LoadAllColumns = true

	// test that the cached query doesnt already exist
	LoadAndQueryRecords(nt, &loadSpec, nil)
	for _, b := range nt.BlockList {
		loaded := LoadCachedResults(&querySpec, b.Name)
		if loaded == true {
			test.Error("Test DB started with saved query results")
		}
	}

	// test that the cached query is saved
	LoadAndQueryRecords(nt, &loadSpec, &querySpec)
	for _, b := range nt.BlockList {
		loaded := LoadCachedResults(&querySpec, b.Name)
		if loaded != true {
			test.Error("Did not correctly save and load query results")
		}
	}

	config.FLAGS.CACHED_QUERIES = &config.FALSE
	for _, b := range nt.BlockList {
		loaded := LoadCachedResults(&querySpec, b.Name)
		if loaded == true {
			test.Error("Used query cache when flag was not provided")
		}
	}
	config.FLAGS.CACHED_QUERIES = &config.TRUE

	// test that a new and slightly different query isnt cached for us
	LoadAndQueryRecords(nt, &loadSpec, nil)
	querySpec.Aggregations = append(aggs, AggregationForTable(nt, "id", "hist"))
	for _, b := range nt.BlockList {
		loaded := LoadCachedResults(&querySpec, b.Name)
		if loaded == true {
			test.Error("Test DB has query results for new query")
		}
	}

}

func testCachedQueryConsistency(test *testing.T) {
	nt := GetTable(TEST_TABLE_NAME)
	filters := []Filter{}
	filters = append(filters, MakeIntFilter(nt, "age", "lt", 20))

	aggs := []Aggregation{}
	aggs = append(aggs, AggregationForTable(nt, "age", "hist"))

	querySpec := QuerySpec{Table: nt,
		QueryParams: QueryParams{Filters: filters, Aggregations: aggs}}
	loadSpec := NewLoadSpec()
	loadSpec.LoadAllColumns = true

	LoadAndQueryRecords(nt, &loadSpec, &querySpec)
	copySpec := CopyQuerySpec(&querySpec)

	nt = GetTable(TEST_TABLE_NAME)

	// clear the copied query spec result map and look
	// at the cached query results

	copySpec.Results = make(ResultMap, 0)
	LoadAndQueryRecords(nt, &loadSpec, copySpec)

	if len(querySpec.Results) == 0 {
		test.Error("No Results for Query")
	}

	for k, v := range querySpec.Results {
		v2, ok := copySpec.Results[k]
		if !ok {
			test.Error("Result Mismatch!", k, v)
		}

		if v.Count != v2.Count {
			test.Error("Count Mismatch", v, v2, v.Count, v2.Count)
		}

		if v.Samples != v2.Samples {
			common.Debug(v, v2)
			test.Error("Samples Mismatch", v, v2, v.Samples, v2.Samples)
		}

	}

	for _, b := range nt.BlockList {
		loaded := LoadCachedResults(&querySpec, b.Name)
		if loaded != true {
			test.Error("Did not correctly save and load query results")
		}
	}

}

func testCachedBasicHist(test *testing.T) {
	nt := GetTable(TEST_TABLE_NAME)

	for _, histType := range []string{"basic", "loghist"} {
		// set query flags as early as possible
		if histType == "loghist" {
			config.FLAGS.LOG_HIST = &config.TRUE
		} else {
			config.FLAGS.LOG_HIST = &config.FALSE
		}

		HIST := "hist"
		config.FLAGS.OP = &HIST

		filters := []Filter{}
		filters = append(filters, MakeIntFilter(nt, "age", "lt", 20))
		aggs := []Aggregation{}
		aggs = append(aggs, AggregationForTable(nt, "age", "hist"))

		querySpec := QuerySpec{Table: nt,
			QueryParams: QueryParams{Filters: filters, Aggregations: aggs}}

		loadSpec := NewLoadSpec()
		loadSpec.LoadAllColumns = true

		LoadAndQueryRecords(nt, &loadSpec, &querySpec)
		copySpec := CopyQuerySpec(&querySpec)

		nt = GetTable(TEST_TABLE_NAME)

		// clear the copied query spec result map and look
		// at the cached query results

		copySpec.Results = make(ResultMap, 0)
		LoadAndQueryRecords(nt, &loadSpec, copySpec)

		if len(querySpec.Results) == 0 {
			test.Error("No Results for Query")
		}

		for k, v := range querySpec.Results {
			v2, ok := copySpec.Results[k]
			if !ok {
				test.Error("Result Mismatch!", histType, k, v)
			}

			if v.Count != v2.Count {
				test.Error("Count Mismatch", histType, v, v2, v.Count, v2.Count)
			}

			if v.Samples != v2.Samples {
				common.Debug(v, v2)
				test.Error("Samples Mismatch", histType, v, v2, v.Samples, v2.Samples)
			}

			for k, h := range v.Hists {
				h2, ok := v2.Hists[k]
				if !ok {
					test.Error("Missing Histogram", histType, v, v2)
				}

				if h.StdDev() <= 0 {
					test.Error("Missing StdDev", histType, h, h.StdDev())
				}

				if math.Abs(h.StdDev()-h2.StdDev()) > 0.1 {
					test.Error("StdDev MisMatch", histType, h, h2)
				}

			}

		}

		for _, b := range nt.BlockList {
			loaded := LoadCachedResults(&querySpec, b.Name)
			if loaded != true {
				test.Error("Did not correctly save and load query results")
			}
		}
	}

}
