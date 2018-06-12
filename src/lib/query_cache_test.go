package sybil

import "testing"
import "math/rand"
import "math"
import "strconv"

func TestCachedQueries(t *testing.T) {
	tableName := getTestTableName(t)
	deleteTestDb(tableName)

	blockCount := 5

	DELETE_BLOCKS_AFTER_QUERY = false
	FLAGS.CACHED_QUERIES = true

	var thisAddRecords = func(block_count int) {
		addRecords(tableName, func(r *Record, i int) {
			age := int64(rand.Intn(20)) + 10

			ageStr := strconv.FormatInt(int64(age), 10)
			r.AddIntField("id", int64(i))
			r.AddIntField("age", age)
			r.AddStrField("age_str", ageStr)
			r.AddSetField("age_set", []string{ageStr})

		}, block_count)
		saveAndReloadTable(t, tableName, block_count)

	}

	thisAddRecords(blockCount)
	testCachedQueryFiles(t, tableName)
	deleteTestDb(tableName)

	thisAddRecords(blockCount)
	testCachedQueryConsistency(t, tableName)
	deleteTestDb(tableName)

	thisAddRecords(blockCount)
	testCachedBasicHist(t, tableName)
	deleteTestDb(tableName)

	FLAGS.CACHED_QUERIES = false
}

func testCachedQueryFiles(t *testing.T, tableName string) {
	nt := GetTable(tableName)
	filters := []Filter{}
	filters = append(filters, nt.IntFilter("age", "lt", 20))

	aggs := []Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "hist"))

	querySpec := QuerySpec{Table: nt,
		QueryParams: QueryParams{Filters: filters, Aggregations: aggs}}
	loadSpec := NewLoadSpec()
	loadSpec.LoadAllColumns = true

	// test that the cached query doesnt already exist
	nt.LoadAndQueryRecords(&loadSpec, nil)
	for _, b := range nt.BlockList {
		loaded := querySpec.LoadCachedResults(b.Name)
		if loaded == true {
			t.Error("Test DB started with saved query results")
		}
	}

	// test that the cached query is saved
	nt.LoadAndQueryRecords(&loadSpec, &querySpec)
	for _, b := range nt.BlockList {
		loaded := querySpec.LoadCachedResults(b.Name)
		if loaded != true {
			t.Error("Did not correctly save and load query results")
		}
	}

	FLAGS.CACHED_QUERIES = false
	for _, b := range nt.BlockList {
		loaded := querySpec.LoadCachedResults(b.Name)
		if loaded == true {
			t.Error("Used query cache when flag was not provided")
		}
	}
	FLAGS.CACHED_QUERIES = true

	// test that a new and slightly different query isnt cached for us
	nt.LoadAndQueryRecords(&loadSpec, nil)
	querySpec.Aggregations = append(aggs, nt.Aggregation("id", "hist"))
	for _, b := range nt.BlockList {
		loaded := querySpec.LoadCachedResults(b.Name)
		if loaded == true {
			t.Error("Test DB has query results for new query")
		}
	}

}

func testCachedQueryConsistency(t *testing.T, tableName string) {
	nt := GetTable(tableName)
	filters := []Filter{}
	filters = append(filters, nt.IntFilter("age", "lt", 20))

	aggs := []Aggregation{}
	aggs = append(aggs, nt.Aggregation("age", "hist"))

	querySpec := QuerySpec{Table: nt,
		QueryParams: QueryParams{Filters: filters, Aggregations: aggs}}
	loadSpec := NewLoadSpec()
	loadSpec.LoadAllColumns = true

	nt.LoadAndQueryRecords(&loadSpec, &querySpec)
	copySpec := CopyQuerySpec(&querySpec)

	nt = GetTable(tableName)

	// clear the copied query spec result map and look
	// at the cached query results

	copySpec.Results = make(ResultMap, 0)
	nt.LoadAndQueryRecords(&loadSpec, copySpec)

	if len(querySpec.Results) == 0 {
		t.Error("No Results for Query")
	}

	for k, v := range querySpec.Results {
		v2, ok := copySpec.Results[k]
		if !ok {
			t.Error("Result Mismatch!", k, v)
		}

		if v.Count != v2.Count {
			t.Error("Count Mismatch", v, v2, v.Count, v2.Count)
		}

		if v.Samples != v2.Samples {
			Debug(v, v2)
			t.Error("Samples Mismatch", v, v2, v.Samples, v2.Samples)
		}

	}

	for _, b := range nt.BlockList {
		loaded := querySpec.LoadCachedResults(b.Name)
		if loaded != true {
			t.Error("Did not correctly save and load query results")
		}
	}

}

func testCachedBasicHist(t *testing.T, tableName string) {
	nt := GetTable(tableName)

	for _, histType := range []string{"basic", "loghist"} {
		// set query flags as early as possible
		if histType == "loghist" {
			FLAGS.LOG_HIST = true
		} else {
			FLAGS.LOG_HIST = false
		}

		HIST := "hist"
		FLAGS.OP = HIST

		filters := []Filter{}
		filters = append(filters, nt.IntFilter("age", "lt", 20))
		aggs := []Aggregation{}
		aggs = append(aggs, nt.Aggregation("age", "hist"))

		querySpec := QuerySpec{Table: nt,
			QueryParams: QueryParams{Filters: filters, Aggregations: aggs}}

		loadSpec := NewLoadSpec()
		loadSpec.LoadAllColumns = true

		nt.LoadAndQueryRecords(&loadSpec, &querySpec)
		copySpec := CopyQuerySpec(&querySpec)

		nt = GetTable(tableName)

		// clear the copied query spec result map and look
		// at the cached query results

		copySpec.Results = make(ResultMap, 0)
		nt.LoadAndQueryRecords(&loadSpec, copySpec)

		if len(querySpec.Results) == 0 {
			t.Error("No Results for Query")
		}

		for k, v := range querySpec.Results {
			v2, ok := copySpec.Results[k]
			if !ok {
				t.Error("Result Mismatch!", histType, k, v)
			}

			if v.Count != v2.Count {
				t.Error("Count Mismatch", histType, v, v2, v.Count, v2.Count)
			}

			if v.Samples != v2.Samples {
				Debug(v, v2)
				t.Error("Samples Mismatch", histType, v, v2, v.Samples, v2.Samples)
			}

			for k, h := range v.Hists {
				h2, ok := v2.Hists[k]
				if !ok {
					t.Error("Missing Histogram", histType, v, v2)
				}

				if h.StdDev() <= 0 {
					t.Error("Missing StdDev", histType, h, h.StdDev())
				}

				if math.Abs(h.StdDev()-h2.StdDev()) > 0.1 {
					t.Error("StdDev MisMatch", histType, h, h2)
				}

			}

		}

		for _, b := range nt.BlockList {
			loaded := querySpec.LoadCachedResults(b.Name)
			if loaded != true {
				t.Error("Did not correctly save and load query results")
			}
		}
	}

}

func TestCacheKeyGeneration(t *testing.T) {
	tests := []struct {
		name string
		qp   QueryParams
		want string
	}{
		{
			"empty",
			QueryParams{},
			"99914b932bd37a50b983c5e7c90ae93b",
		},
		{
			"with-replacements",
			QueryParams{StrReplace: map[string]StrReplace{
				"a": StrReplace{},
				"b": StrReplace{},
			}},
			"756ffe5df4c1293316de80ac7b0977ab",
		},
	}

	// TODO: once we're on go1.7 use t.Run
	for _, tt := range tests {
		if got := tt.qp.cacheKey(); got != tt.want {
			t.Errorf("%q. cacheKey = \n%v, want \n%v", tt.name, got, tt.want)

		}
	}
}
