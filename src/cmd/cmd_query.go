package sybil_cmd

import (
	"flag"
	"path"
	"runtime/debug"
	"strings"
	"time"

	sybil "github.com/logv/sybil/src/lib"
)

var MAX_RECORDS_NO_GC = 4 * 1000 * 1000 // 4 million

const (
	SORT_COUNT = sybil.SORT_COUNT
)

func addPrintFlags() {
	flag.StringVar(&sybil.FLAGS.OP, "op", "avg", "metric to calculate, either 'avg' or 'hist'")
	flag.BoolVar(&sybil.FLAGS.LIST_TABLES, "tables", false, "List tables")
	flag.BoolVar(&sybil.FLAGS.PRINT_INFO, "info", false, "Print table info")
	flag.IntVar(&sybil.FLAGS.LIMIT, "limit", 100, "Number of results to return")
	flag.BoolVar(&sybil.FLAGS.PRINT, "print", true, "Print some records")
	flag.BoolVar(&sybil.FLAGS.SAMPLES, "samples", false, "Grab samples")
	flag.BoolVar(&sybil.FLAGS.JSON, "json", false, "Print results in JSON format")
}

func addQueryFlags() {
	if sybil.ENABLE_TDIGEST {
		flag.BoolVar(&sybil.FLAGS.T_DIGEST, "tdigest", false, "Use TDIGEST Histograms")
	}

	flag.StringVar(&sybil.FLAGS.SORT, "sort", SORT_COUNT, "Int Column to sort by")
	flag.StringVar(&sybil.FLAGS.PRUNE_BY, "prune-sort", SORT_COUNT, "Int Column to prune intermediate results by")

	flag.BoolVar(&sybil.FLAGS.TIME, "time", false, "make a time rollup")
	flag.StringVar(&sybil.FLAGS.TIME_COL, "time-col", "time", "which column to treat as a timestamp (use with -time flag)")
	flag.IntVar(&sybil.FLAGS.TIME_BUCKET, "time-bucket", 60*60, "time bucket (in seconds)")
	flag.StringVar(&sybil.FLAGS.WEIGHT_COL, "weight-col", "", "Which column to treat as an optional weighting column")

	flag.BoolVar(&sybil.FLAGS.LOG_HIST, "loghist", false, "Use nested logarithmic histograms")

	flag.BoolVar(&sybil.FLAGS.ENCODE_RESULTS, "encode-results", false, "Print the results in binary format")
	flag.BoolVar(&sybil.FLAGS.ENCODE_FLAGS, "encode-flags", false, "Print the query flags in binary format")
	flag.BoolVar(&sybil.FLAGS.DECODE_FLAGS, "decode-flags", false, "Use the query flags supplied on stdin")
	flag.StringVar(&sybil.FLAGS.INT_FILTERS, "int-filter", "", "Int filters, format: col:op:val")
	flag.IntVar(&sybil.FLAGS.HIST_BUCKET, "int-bucket", 0, "Int hist bucket size")

	flag.StringVar(&sybil.FLAGS.STR_REPLACE, "str-replace", "", "Str replacement, format: col:find:replace")
	flag.StringVar(&sybil.FLAGS.STR_FILTERS, "str-filter", "", "Str filters, format: col:op:val")
	flag.StringVar(&sybil.FLAGS.SET_FILTERS, "set-filter", "", "Set filters, format: col:op:val")
	flag.BoolVar(&sybil.FLAGS.UPDATE_TABLE_INFO, "update-info", false, "Re-compute cached column data")

	flag.StringVar(&sybil.FLAGS.INTS, "int", "", "Integer values to aggregate")
	flag.StringVar(&sybil.FLAGS.FLOATS, "float", "", "Float values to aggregate")
	flag.StringVar(&sybil.FLAGS.STRS, "str", "", "String values to load")
	flag.StringVar(&sybil.FLAGS.SETS, "set", "", "Set values to load")
	flag.StringVar(&sybil.FLAGS.SAMPLE_COLS, "sample-cols", "", "Columns to load for samples query")
	flag.StringVar(&sybil.FLAGS.GROUPS, "group", "", "values group by")
	flag.StringVar(&sybil.FLAGS.DISTINCT, sybil.DISTINCT_STR, "", "distinct group by")
	flag.IntVar(&sybil.FLAGS.NUM_DISTINCT, sybil.NUM_DISTINCT, -1, "short the group by when this number of elements is hit")

	flag.BoolVar(&sybil.FLAGS.EXPORT, "export", false, "export data to TSV")

	flag.BoolVar(&sybil.FLAGS.READ_ROWSTORE, "read-log", false, "read the ingestion log (can take longer!)")

	flag.BoolVar(&sybil.FLAGS.RECYCLE_MEM, "recycle-mem", true, "recycle memory slabs (versus using Go's GC)")
	flag.BoolVar(&sybil.FLAGS.FAST_RECYCLE, "fast-recycle", true, "faster memory recycling")
	flag.BoolVar(&sybil.FLAGS.SHORTEN_KEY_TABLE, "shorten-key-table", true, "faster queries on wide tabes by shortening the key lookup")

	flag.BoolVar(&sybil.FLAGS.CACHED_QUERIES, "cache-queries", false, "Cache query results per block")

}

func RunQueryCmdLine() {
	addQueryFlags()
	addPrintFlags()
	flag.Parse()

	runQueryCmdLine()

}

func runQueryCmdLine() {
	if sybil.FLAGS.DECODE_FLAGS {
		sybil.DecodeFlags()
	}

	if sybil.FLAGS.ENCODE_FLAGS {
		sybil.Debug("PRINTING ENCODED FLAGS")
		sybil.EncodeFlags()
		return
	}

	if sybil.FLAGS.LIST_TABLES {
		sybil.PrintTables()
		return
	}

	table := sybil.FLAGS.TABLE
	if table == "" {
		flag.PrintDefaults()
		return
	}

	t := sybil.GetTable(table)
	if t.IsNotExist() {
		sybil.Error(t.Name, "table can not be loaded or does not exist in", sybil.FLAGS.DIR)
	}

	ints := make([]string, 0)
	floats := make([]string, 0)
	groups := make([]string, 0)
	strs := make([]string, 0)
	sets := make([]string, 0)
	distinct := make([]string, 0)

	has_sample_cols := false

	if sybil.FLAGS.GROUPS != "" {
		groups = strings.Split(sybil.FLAGS.GROUPS, sybil.FLAGS.FIELD_SEPARATOR)
	}

	if sybil.FLAGS.DISTINCT != "" {
		distinct = strings.Split(sybil.FLAGS.DISTINCT, sybil.FLAGS.FIELD_SEPARATOR)
	}

	// PROCESS CMD LINE ARGS THAT USE COMMA DELIMITERS
	if sybil.FLAGS.STRS != "" {
		strs = strings.Split(sybil.FLAGS.STRS, sybil.FLAGS.FIELD_SEPARATOR)
		has_sample_cols = true
	}
	if sybil.FLAGS.INTS != "" {
		ints = strings.Split(sybil.FLAGS.INTS, sybil.FLAGS.FIELD_SEPARATOR)
		has_sample_cols = true
	}
	if sybil.FLAGS.FLOATS != "" {
		floats = strings.Split(sybil.FLAGS.FLOATS, sybil.FLAGS.FIELD_SEPARATOR)
		has_sample_cols = true
	}
	if sybil.FLAGS.SETS != "" {
		sets = strings.Split(sybil.FLAGS.SETS, sybil.FLAGS.FIELD_SEPARATOR)
		has_sample_cols = true
	}

	sample_cols := make([]string, 0)
	if sybil.FLAGS.SAMPLE_COLS != "" {
		sample_cols = strings.Split(sybil.FLAGS.SAMPLE_COLS, sybil.FLAGS.FIELD_SEPARATOR)

		has_sample_cols = true

	}

	if sybil.FLAGS.PROFILE && sybil.PROFILER_ENABLED {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	if sybil.FLAGS.READ_ROWSTORE {
		sybil.FLAGS.READ_INGESTION_LOG = true
	}

	// LOAD TABLE INFOS BEFORE WE CREATE OUR FILTERS, SO WE CAN CREATE FILTERS ON
	// THE RIGHT COLUMN ID
	t.LoadTableInfo()
	t.LoadRecords(nil)

	// Make filterSpec before shortening key table
	filterSpec := sybil.FilterSpec{Int: sybil.FLAGS.INT_FILTERS, Str: sybil.FLAGS.STR_FILTERS, Set: sybil.FLAGS.SET_FILTERS}

	count := 0
	for _, block := range t.BlockList {
		count += int(block.Info.NumRecords)
	}

	sybil.Debug("WILL INSPECT", count, "RECORDS")

	// {{ TRIM KEY TABLE
	shorten_key_table := sybil.FLAGS.SHORTEN_KEY_TABLE
	if sybil.FLAGS.PRINT_INFO {
		shorten_key_table = false
	}
	if sybil.FLAGS.SAMPLES && !has_sample_cols {
		shorten_key_table = false
	}

	if sybil.FLAGS.READ_ROWSTORE && shorten_key_table {
		sybil.Debug("DISABLING SHORT KEY TABLE BECAUSE OF ROWSTORE")
		shorten_key_table = false
	}
	sybil.Debug("SHORTEN KEY TABLE?", shorten_key_table)

	if shorten_key_table {
		t.UseKeys(strs)
		t.UseKeys(strs)
		t.UseKeys(sets)
		t.UseKeys(ints)
		t.UseKeys(floats)
		t.UseKeys(groups)
		t.UseKeys(distinct)
		t.UseKeys(sample_cols)
		t.UseKeys(filterSpec.GetFilterCols())

		t.ShortenKeyTable()

	}
	// }}

	groupings := []sybil.Grouping{}
	for _, g := range groups {
		groupings = append(groupings, t.Grouping(g))
	}

	aggs := []sybil.Aggregation{}
	if !sybil.FLAGS.SAMPLES {
		for _, agg := range ints {
			aggs = append(aggs, t.Aggregation(agg, sybil.FLAGS.OP))
		}
		for _, agg := range floats {
			aggs = append(aggs, t.Aggregation(agg, sybil.FLAGS.OP))
		}
	}

	distincts := []sybil.Grouping{}
	for _, g := range distinct {
		distincts = append(distincts, t.Grouping(g))
	}

	if sybil.FLAGS.OP == sybil.DISTINCT_STR {
		distincts = groupings
		groupings = make([]sybil.Grouping, 0)
	}

	// VERIFY THE KEY TABLE IS IN ORDER, OTHERWISE WE NEED TO EXIT
	sybil.Debug("KEY TABLE", t.KeyTable)
	sybil.Debug("KEY TYPES", t.KeyTypes)

	used := make(map[int16]int)
	for _, v := range t.KeyTable {
		used[v]++
		if used[v] > 1 {
			sybil.Error("THERE IS A SERIOUS KEY TABLE INCONSISTENCY")
			return
		}
	}

	loadSpec := t.NewLoadSpec()
	filters := sybil.BuildFilters(t, &loadSpec, filterSpec)

	query_params := sybil.QueryParams{Groups: groupings, Filters: filters,
		Aggregations: aggs, Distincts: distincts}

	querySpec := sybil.QuerySpec{QueryParams: query_params}

	all_groups := append(groups, distinct...)
	for _, v := range all_groups {
		switch t.GetColumnType(v) {
		case sybil.STR_VAL:
			loadSpec.Str(v)
		case sybil.INT_VAL:
			loadSpec.Int(v)
		case sybil.SET_VAL:
			sybil.Error("Grouping by Set columns is currently not supported")
		default:
			t.PrintTableInfo()
			loadSpec.Missing(v)
		}

	}

	for _, v := range sample_cols {
		key_id := t.KeyTable[v]
		switch t.KeyTypes[key_id] {
		case sybil.INT_VAL:
			ints = append(ints, v)
		case sybil.STR_VAL:
			strs = append(strs, v)
		case sybil.SET_VAL:
			sets = append(sets, v)
		case sybil.FLOAT_VAL:
			floats = append(floats, v)

		}
	}

	for _, v := range strs {
		loadSpec.Str(v)
	}
	for _, v := range sets {
		loadSpec.Set(v)
	}
	for _, v := range ints {
		loadSpec.Int(v)
	}
	for _, v := range floats {
		loadSpec.Float(v)
	}

	if sybil.FLAGS.SORT != "" {
		if sybil.FLAGS.SORT != SORT_COUNT {
			loadSpec.Int(sybil.FLAGS.SORT)
		}
		querySpec.OrderBy = sybil.FLAGS.SORT
	} else {
		querySpec.OrderBy = ""
	}

	if sybil.FLAGS.PRUNE_BY != "" {
		if sybil.FLAGS.PRUNE_BY != SORT_COUNT {
			loadSpec.Int(sybil.FLAGS.PRUNE_BY)
		}
		querySpec.PruneBy = sybil.FLAGS.PRUNE_BY
	} else {
		querySpec.PruneBy = SORT_COUNT
	}

	if sybil.FLAGS.TIME {
		// TODO: infer the TimeBucket size
		querySpec.TimeBucket = sybil.FLAGS.TIME_BUCKET
		sybil.Debug("USING TIME BUCKET", querySpec.TimeBucket, "SECONDS")
		loadSpec.Int(sybil.FLAGS.TIME_COL)
		time_col_id, ok := t.KeyTable[sybil.FLAGS.TIME_COL]
		if ok {
			sybil.OPTS.TIME_COL_ID = time_col_id
		}
	}

	if sybil.FLAGS.WEIGHT_COL != "" {
		sybil.OPTS.WEIGHT_COL = true
		loadSpec.Int(sybil.FLAGS.WEIGHT_COL)
		sybil.OPTS.WEIGHT_COL_ID = t.KeyTable[sybil.FLAGS.WEIGHT_COL]
	}

	querySpec.Limit = int(sybil.FLAGS.LIMIT)
	querySpec.NumDistinct = int(sybil.FLAGS.NUM_DISTINCT)
	if querySpec.NumDistinct > 0 {
		sybil.Debug("Setting Limit to same as NumDistinct:", querySpec.NumDistinct)
		querySpec.Limit = querySpec.NumDistinct
	}

	if sybil.FLAGS.SAMPLES {
		sybil.HOLD_MATCHES = true
		sybil.DELETE_BLOCKS_AFTER_QUERY = false

		if !has_sample_cols {
			loadSpec = t.NewLoadSpec()
			loadSpec.LoadAllColumns = true
		}

		t.LoadAndQueryRecords(&loadSpec, &querySpec)

		t.PrintSamples()

		return
	}

	if sybil.FLAGS.EXPORT {
		loadSpec.LoadAllColumns = true
	}

	if !sybil.FLAGS.PRINT_INFO {
		// DISABLE GC FOR QUERY PATH
		sybil.Debug("ADDING BULLET HOLES FOR SPEED (DISABLING GC)")
		debug.SetGCPercent(-1)

		sybil.Debug("USING LOAD SPEC", loadSpec)

		sybil.Debug("USING QUERY SPEC", querySpec)

		start := time.Now()
		// We can load and query at the same time
		if sybil.FLAGS.LOAD_AND_QUERY {
			count = t.LoadAndQueryRecords(&loadSpec, &querySpec)

			end := time.Now()
			sybil.Debug("LOAD AND QUERY RECORDS TOOK", end.Sub(start))
			querySpec.PrintResults()
		}

	}

	if sybil.FLAGS.EXPORT {
		sybil.Print("EXPORTED RECORDS TO", path.Join(t.Name, "export"))
	}

	if sybil.FLAGS.PRINT_INFO {
		t := sybil.GetTable(table)
		sybil.FLAGS.LOAD_AND_QUERY = false

		t.PrintTableInfo()
	}

}

func split(s, sep string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, sep)
}
