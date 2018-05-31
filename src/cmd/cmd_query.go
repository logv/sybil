package cmd

import (
	"flag"
	"fmt"
	"path"
	"runtime/debug"
	"strings"
	"time"

	"github.com/logv/sybil/src/sybil"
)

var MAX_RECORDS_NO_GC = 4 * 1000 * 1000 // 4 million

var NO_RECYCLE_MEM *bool

const (
	SORT_COUNT = "$COUNT"
)

func addQueryFlags() {

	sybil.FLAGS.PRINT_INFO = flag.Bool("info", false, "Print table info")
	sybil.FLAGS.SORT = flag.String("sort", SORT_COUNT, "Int Column to sort by")
	sybil.FLAGS.PRUNE_BY = flag.String("prune-sort", SORT_COUNT, "Int Column to prune intermediate results by")

	sybil.FLAGS.LIMIT = flag.Int("limit", 100, "Number of results to return")

	sybil.FLAGS.TIME = flag.Bool("time", false, "make a time rollup")
	sybil.FLAGS.TIME_COL = flag.String("time-col", "time", "which column to treat as a timestamp (use with -time flag)")
	sybil.FLAGS.TIME_BUCKET = flag.Int("time-bucket", 60*60, "time bucket (in seconds)")
	sybil.FLAGS.WEIGHT_COL = flag.String("weight-col", "", "Which column to treat as an optional weighting column")

	sybil.FLAGS.OP = flag.String("op", "avg", "metric to calculate, either 'avg' or 'hist'")
	sybil.FLAGS.LOG_HIST = flag.Bool("loghist", false, "Use nested logarithmic histograms")

	sybil.FLAGS.PRINT = flag.Bool("print", true, "Print some records")
	sybil.FLAGS.ENCODE_RESULTS = flag.Bool("encode-results", false, "Print the results in binary format")
	sybil.FLAGS.ENCODE_FLAGS = flag.Bool("encode-flags", false, "Print the query flags in binary format")
	sybil.FLAGS.DECODE_FLAGS = flag.Bool("decode-flags", false, "Use the query flags supplied on stdin")
	sybil.FLAGS.SAMPLES = flag.Bool("samples", false, "Grab samples")
	sybil.FLAGS.INT_FILTERS = flag.String("int-filter", "", "Int filters, format: col:op:val")

	sybil.FLAGS.HIST_BUCKET = flag.Int("int-bucket", 0, "Int hist bucket size")

	sybil.FLAGS.STR_REPLACE = flag.String("str-replace", "", "Str replacement, format: col:find:replace")
	sybil.FLAGS.STR_FILTERS = flag.String("str-filter", "", "Str filters, format: col:op:val")
	sybil.FLAGS.SET_FILTERS = flag.String("set-filter", "", "Set filters, format: col:op:val")
	sybil.FLAGS.UPDATE_TABLE_INFO = flag.Bool("update-info", false, "Re-compute cached column data")

	sybil.FLAGS.INTS = flag.String("int", "", "Integer values to aggregate")
	sybil.FLAGS.STRS = flag.String("str", "", "String values to load")
	sybil.FLAGS.GROUPS = flag.String("group", "", "values group by")
	sybil.FLAGS.DISTINCT = flag.String(sybil.DISTINCT_STR, "", "distinct group by")

	sybil.FLAGS.EXPORT = flag.Bool("export", false, "export data to TSV")

	sybil.FLAGS.READ_ROWSTORE = flag.Bool("read-log", false, "read the ingestion log (can take longer!)")

	sybil.FLAGS.JSON = flag.Bool("json", false, "Print results in JSON format")
	sybil.FLAGS.ANOVA_ICC = flag.Bool("icc", false, "Calculate intraclass co-efficient (ANOVA)")

	sybil.FLAGS.LIST_TABLES = flag.Bool("tables", false, "List tables")

	NO_RECYCLE_MEM = flag.Bool("no-recycle-mem", false, "don't recycle memory slabs (use Go GC instead)")

	sybil.FLAGS.CACHED_QUERIES = flag.Bool("cache-queries", false, "Cache query results per block")

}

func RunQueryCmdLine() {
	addQueryFlags()
	flag.Parse()

	if *sybil.FLAGS.DECODE_FLAGS {
		sybil.DecodeFlags()
	}

	if *sybil.FLAGS.ENCODE_FLAGS {
		sybil.Debug("PRINTING ENCODED FLAGS")
		sybil.EncodeFlags()
		return
	}

	if *sybil.FLAGS.LIST_TABLES {
		sybil.PrintTables()
		return
	}

	table := *sybil.FLAGS.TABLE
	if table == "" {
		flag.PrintDefaults()
		return
	}

	t := sybil.GetTable(table)
	if t.IsNotExist() {
		sybil.Error(t.Name, "table can not be loaded or does not exist in", *sybil.FLAGS.DIR)
	}

	ints := make([]string, 0)
	groups := make([]string, 0)
	strs := make([]string, 0)
	distinct := make([]string, 0)

	if *sybil.FLAGS.GROUPS != "" {
		groups = strings.Split(*sybil.FLAGS.GROUPS, *sybil.FLAGS.FIELD_SEPARATOR)
		sybil.OPTS.GROUP_BY = groups
	}

	if *sybil.FLAGS.DISTINCT != "" {
		distinct = strings.Split(*sybil.FLAGS.DISTINCT, *sybil.FLAGS.FIELD_SEPARATOR)
		sybil.OPTS.DISTINCT = distinct
	}

	if *NO_RECYCLE_MEM {
		sybil.FLAGS.RECYCLE_MEM = sybil.NewFalseFlag()
	}

	// PROCESS CMD LINE ARGS THAT USE COMMA DELIMITERS
	if *sybil.FLAGS.STRS != "" {
		strs = strings.Split(*sybil.FLAGS.STRS, *sybil.FLAGS.FIELD_SEPARATOR)
	}
	if *sybil.FLAGS.INTS != "" {
		ints = strings.Split(*sybil.FLAGS.INTS, *sybil.FLAGS.FIELD_SEPARATOR)
	}
	if *sybil.FLAGS.PROFILE && sybil.PROFILER_ENABLED {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	if *sybil.FLAGS.READ_ROWSTORE {
		sybil.FLAGS.READ_INGESTION_LOG = sybil.NewTrueFlag()
	}

	// LOAD TABLE INFOS BEFORE WE CREATE OUR FILTERS, SO WE CAN CREATE FILTERS ON
	// THE RIGHT COLUMN ID
	t.LoadTableInfo()
	t.LoadRecords(nil)

	count := 0
	for _, block := range t.BlockList {
		count += int(block.Info.NumRecords)
	}

	sybil.Debug("WILL INSPECT", count, "RECORDS")

	groupings := []sybil.Grouping{}
	for _, g := range groups {
		groupings = append(groupings, t.Grouping(g))
	}

	aggs := []sybil.Aggregation{}
	for _, agg := range ints {
		aggs = append(aggs, t.Aggregation(agg, *sybil.FLAGS.OP))
	}

	distincts := []sybil.Grouping{}
	for _, g := range distinct {
		distincts = append(distincts, t.Grouping(g))
	}

	if *sybil.FLAGS.OP == sybil.DISTINCT_STR {
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
	filterSpec := sybil.FilterSpec{Int: *sybil.FLAGS.INT_FILTERS, Str: *sybil.FLAGS.STR_FILTERS, Set: *sybil.FLAGS.SET_FILTERS}
	filters := sybil.BuildFilters(t, &loadSpec, filterSpec)

	queryParams := sybil.QueryParams{Groups: groupings, Filters: filters,
		Aggregations: aggs, Distincts: distincts}

	querySpec := sybil.QuerySpec{QueryParams: queryParams}

	allGroups := append(groups, distinct...)
	for _, v := range allGroups {
		switch t.GetColumnType(v) {
		case sybil.STR_VAL:
			loadSpec.Str(v)
		case sybil.INT_VAL:
			loadSpec.Int(v)
		default:
			t.PrintColInfo()
			fmt.Println("")
			sybil.Error("Unknown column type for column: ", v, t.GetColumnType(v))
		}

	}
	for _, v := range strs {
		loadSpec.Str(v)
	}
	for _, v := range ints {
		loadSpec.Int(v)
	}

	if *sybil.FLAGS.SORT != "" {
		if *sybil.FLAGS.SORT != sybil.OPTS.SORT_COUNT {
			loadSpec.Int(*sybil.FLAGS.SORT)
		}
		querySpec.OrderBy = *sybil.FLAGS.SORT
	} else {
		querySpec.OrderBy = ""
	}

	if *sybil.FLAGS.PRUNE_BY != "" {
		if *sybil.FLAGS.PRUNE_BY != sybil.OPTS.SORT_COUNT {
			loadSpec.Int(*sybil.FLAGS.PRUNE_BY)
		}
		querySpec.PruneBy = *sybil.FLAGS.PRUNE_BY
	} else {
		querySpec.PruneBy = sybil.OPTS.SORT_COUNT
	}

	if *sybil.FLAGS.TIME {
		// TODO: infer the TimeBucket size
		querySpec.TimeBucket = *sybil.FLAGS.TIME_BUCKET
		sybil.Debug("USING TIME BUCKET", querySpec.TimeBucket, "SECONDS")
		loadSpec.Int(*sybil.FLAGS.TIME_COL)
		timeColID, ok := t.KeyTable[*sybil.FLAGS.TIME_COL]
		if ok {
			sybil.OPTS.TIME_COL_ID = timeColID
		}
	}

	if *sybil.FLAGS.WEIGHT_COL != "" {
		sybil.OPTS.WEIGHT_COL = true
		loadSpec.Int(*sybil.FLAGS.WEIGHT_COL)
		sybil.OPTS.WEIGHT_COL_ID = t.KeyTable[*sybil.FLAGS.WEIGHT_COL]
	}

	querySpec.Limit = int16(*sybil.FLAGS.LIMIT)

	if *sybil.FLAGS.SAMPLES {
		sybil.HOLD_MATCHES = true
		sybil.DELETE_BLOCKS_AFTER_QUERY = false

		loadSpec := t.NewLoadSpec()
		loadSpec.LoadAllColumns = true

		t.LoadAndQueryRecords(&loadSpec, &querySpec)

		t.PrintSamples()

		return
	}

	if *sybil.FLAGS.EXPORT {
		loadSpec.LoadAllColumns = true
	}

	if !*sybil.FLAGS.PRINT_INFO {
		// DISABLE GC FOR QUERY PATH
		sybil.Debug("ADDING BULLET HOLES FOR SPEED (DISABLING GC)")
		debug.SetGCPercent(-1)

		sybil.Debug("USING LOAD SPEC", loadSpec)

		sybil.Debug("USING QUERY SPEC", querySpec)

		start := time.Now()
		// We can load and query at the same time
		if *sybil.FLAGS.LOAD_AND_QUERY {
			t.LoadAndQueryRecords(&loadSpec, &querySpec)

			end := time.Now()
			sybil.Debug("LOAD AND QUERY RECORDS TOOK", end.Sub(start))
			querySpec.PrintResults()
		}

	}

	if *sybil.FLAGS.EXPORT {
		sybil.Print("EXPORTED RECORDS TO", path.Join(t.Name, "export"))
	}

	if *sybil.FLAGS.PRINT_INFO {
		t := sybil.GetTable(table)
		sybil.FLAGS.LOAD_AND_QUERY = sybil.NewFalseFlag()

		t.LoadRecords(nil)
		t.PrintColInfo()
	}

}
