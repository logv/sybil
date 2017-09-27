package sybil_test

import sybil "./"

import "testing"
import "math/rand"
import "math"
import "strconv"

func TestCachedQueries(test *testing.T) {
	delete_test_db()

	block_count := 5

	sybil.DELETE_BLOCKS_AFTER_QUERY = false
	sybil.FLAGS.CACHED_QUERIES = &sybil.TRUE

	var this_add_records = func(block_count int) {
		add_records(func(r *sybil.Record, i int) {
			age := int64(rand.Intn(20)) + 10

			age_str := strconv.FormatInt(int64(age), 10)
			r.AddIntField("id", int64(i))
			r.AddIntField("age", age)
			r.AddStrField("age_str", age_str)
			r.AddSetField("age_set", []string{age_str})

		}, block_count)
		save_and_reload_table(test, block_count)

	}

	this_add_records(block_count)
	testCachedQueryFiles(test)
	delete_test_db()

	this_add_records(block_count)
	testCachedQueryConsistency(test)
	delete_test_db()

	this_add_records(block_count)
	testCachedBasicHist(test)
	delete_test_db()

	sybil.FLAGS.CACHED_QUERIES = &sybil.FALSE

}

func testCachedQueryFiles(test *testing.T) {
	nt := sybil.GetTable(TEST_TABLE_NAME)
	filters := []sybil.Filter{}
	filters = append(filters, nt.IntFilter("age", "lt", 20))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "hist"))

	querySpec := sybil.QuerySpec{Table: nt,
		QueryParams: sybil.QueryParams{Filters: filters, Aggregations: aggs}}
	loadSpec := sybil.NewLoadSpec()
	loadSpec.LoadAllColumns = true

	// test that the cached query doesnt already exist
	nt.LoadAndQueryRecords(&loadSpec, nil)
	for _, b := range nt.BlockList {
		loaded := querySpec.LoadCachedResults(b.Name)
		if loaded == true {
			test.Error("Test DB started with saved query results")
		}
	}

	// test that the cached query is saved
	nt.LoadAndQueryRecords(&loadSpec, &querySpec)
	for _, b := range nt.BlockList {
		loaded := querySpec.LoadCachedResults(b.Name)
		if loaded != true {
			test.Error("Did not correctly save and load query results")
		}
	}

	sybil.FLAGS.CACHED_QUERIES = &sybil.FALSE
	for _, b := range nt.BlockList {
		loaded := querySpec.LoadCachedResults(b.Name)
		if loaded == true {
			test.Error("Used query cache when flag was not provided")
		}
	}
	sybil.FLAGS.CACHED_QUERIES = &sybil.TRUE

	// test that a new and slightly different query isnt cached for us
	nt.LoadAndQueryRecords(&loadSpec, nil)
	querySpec.Aggregations = append(aggs, nt.Aggregation("id", "hist"))
	for _, b := range nt.BlockList {
		loaded := querySpec.LoadCachedResults(b.Name)
		if loaded == true {
			test.Error("Test DB has query results for new query")
		}
	}

}

func testCachedQueryConsistency(test *testing.T) {
	nt := sybil.GetTable(TEST_TABLE_NAME)
	filters := []sybil.Filter{}
	filters = append(filters, nt.IntFilter("age", "lt", 20))

	aggs := []sybil.Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "hist"))

	querySpec := sybil.QuerySpec{Table: nt,
		QueryParams: sybil.QueryParams{Filters: filters, Aggregations: aggs}}
	loadSpec := sybil.NewLoadSpec()
	loadSpec.LoadAllColumns = true

	nt.LoadAndQueryRecords(&loadSpec, &querySpec)
	copySpec := sybil.CopyQuerySpec(&querySpec)

	nt = sybil.GetTable(TEST_TABLE_NAME)

	// clear the copied query spec result map and look
	// at the cached query results

	copySpec.Results = make(sybil.ResultMap, 0)
	nt.LoadAndQueryRecords(&loadSpec, copySpec)

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
			Debug(v, v2)
			test.Error("Samples Mismatch", v, v2, v.Samples, v2.Samples)
		}

	}

	for _, b := range nt.BlockList {
		loaded := querySpec.LoadCachedResults(b.Name)
		if loaded != true {
			test.Error("Did not correctly save and load query results")
		}
	}

}

func testCachedBasicHist(test *testing.T) {
	nt := sybil.GetTable(TEST_TABLE_NAME)

	for _, hist_type := range []string{"basic", "loghist"} {
		// set query flags as early as possible
		if hist_type == "loghist" {
			sybil.FLAGS.LOG_HIST = &sybil.TRUE
		} else {
			sybil.FLAGS.LOG_HIST = &sybil.FALSE
		}

		HIST := "hist"
		sybil.FLAGS.OP = &HIST

		filters := []sybil.Filter{}
		filters = append(filters, nt.IntFilter("age", "lt", 20))
		aggs := []sybil.Aggregation{}
		aggs = append(aggs, nt.Aggregation("age", "hist"))

		querySpec := sybil.QuerySpec{Table: nt,
			QueryParams: sybil.QueryParams{Filters: filters, Aggregations: aggs}}

		loadSpec := sybil.NewLoadSpec()
		loadSpec.LoadAllColumns = true

		nt.LoadAndQueryRecords(&loadSpec, &querySpec)
		copySpec := sybil.CopyQuerySpec(&querySpec)

		nt = sybil.GetTable(TEST_TABLE_NAME)

		// clear the copied query spec result map and look
		// at the cached query results

		copySpec.Results = make(sybil.ResultMap, 0)
		nt.LoadAndQueryRecords(&loadSpec, copySpec)

		if len(querySpec.Results) == 0 {
			test.Error("No Results for Query")
		}

		for k, v := range querySpec.Results {
			v2, ok := copySpec.Results[k]
			if !ok {
				test.Error("Result Mismatch!", hist_type, k, v)
			}

			if v.Count != v2.Count {
				test.Error("Count Mismatch", hist_type, v, v2, v.Count, v2.Count)
			}

			if v.Samples != v2.Samples {
				Debug(v, v2)
				test.Error("Samples Mismatch", hist_type, v, v2, v.Samples, v2.Samples)
			}

			for k, h := range v.Hists {
				h2, ok := v2.Hists[k]
				if !ok {
					test.Error("Missing Histogram", hist_type, v, v2)
				}

				if h.StdDev() <= 0 {
					test.Error("Missing StdDev", hist_type, h, h.StdDev())
				}

				if math.Abs(h.StdDev()-h2.StdDev()) > 0.1 {
					test.Error("StdDev MisMatch", hist_type, h, h2)
				}

			}

		}

		for _, b := range nt.BlockList {
			loaded := querySpec.LoadCachedResults(b.Name)
			if loaded != true {
				test.Error("Did not correctly save and load query results")
			}
		}
	}

}
