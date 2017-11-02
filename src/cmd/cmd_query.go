package cmd

import (
	"flag"
	"fmt"
	"path"
	"runtime/debug"
	"strings"
	"time"

	luajit "github.com/logv/sybil/src/exp/luajit"
	. "github.com/logv/sybil/src/exp/stats"
	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	md "github.com/logv/sybil/src/lib/metadata"
	. "github.com/logv/sybil/src/lib/structs"
	filters "github.com/logv/sybil/src/query/filters"
	hists "github.com/logv/sybil/src/query/hists"
	. "github.com/logv/sybil/src/query/load_and_query"
	printer "github.com/logv/sybil/src/query/printer"
	specs "github.com/logv/sybil/src/query/specs"
	md_io "github.com/logv/sybil/src/storage/metadata_io"
)

var MAX_RECORDS_NO_GC = 4 * 1000 * 1000 // 4 million

var LIST_TABLES *bool
var TIME_FORMAT *string
var NO_RECYCLE_MEM *bool

func addQueryFlags() {

	FLAGS.PRINT_INFO = flag.Bool("info", false, "Print table info")
	FLAGS.SORT = flag.String("sort", OPTS.SORT_COUNT, "Int Column to sort by")
	FLAGS.LIMIT = flag.Int("limit", 100, "Number of results to return")

	FLAGS.TIME = flag.Bool("time", false, "make a time rollup")
	FLAGS.TIME_COL = flag.String("time-col", "time", "which column to treat as a timestamp (use with -time flag)")
	FLAGS.TIME_BUCKET = flag.Int("time-bucket", 60*60, "time bucket (in seconds)")
	FLAGS.WEIGHT_COL = flag.String("weight-col", "", "Which column to treat as an optional weighting column")

	FLAGS.OP = flag.String("op", "avg", "metric to calculate, either 'avg' or 'hist'")
	FLAGS.LOG_HIST = flag.Bool("loghist", false, "Use nested logarithmic histograms")
	if hists.ENABLE_HDR {
		FLAGS.HDR_HIST = flag.Bool("hdr", false, "Use HDR Histograms (can be slow)")
	}

	FLAGS.PRINT = flag.Bool("print", true, "Print some records")
	FLAGS.SAMPLES = flag.Bool("samples", false, "Grab samples")
	FLAGS.INT_FILTERS = flag.String("int-filter", "", "Int filters, format: col:op:val")

	FLAGS.HIST_BUCKET = flag.Int("int-bucket", 0, "Int hist bucket size")

	FLAGS.STR_REPLACE = flag.String("str-replace", "", "Str replacement, format: col:find:replace")
	FLAGS.STR_FILTERS = flag.String("str-filter", "", "Str filters, format: col:op:val")
	FLAGS.SET_FILTERS = flag.String("set-filter", "", "Set filters, format: col:op:val")
	FLAGS.UPDATE_TABLE_INFO = flag.Bool("update-info", false, "Re-compute cached column data")

	FLAGS.INTS = flag.String("int", "", "Integer values to aggregate")
	FLAGS.STRS = flag.String("str", "", "String values to load")
	FLAGS.GROUPS = flag.String("group", "", "values group by")

	FLAGS.EXPORT = flag.Bool("export", false, "export data to TSV")

	FLAGS.READ_ROWSTORE = flag.Bool("read-log", false, "read the ingestion log (can take longer!)")

	FLAGS.JSON = flag.Bool("json", false, "Print results in JSON format")
	FLAGS.ANOVA_ICC = flag.Bool("icc", false, "Calculate intraclass co-efficient (ANOVA)")

	if ENABLE_LUA {
		FLAGS.LUAFILE = flag.String("lua", "", "Script to execute with lua map reduce")
	}

	LIST_TABLES = flag.Bool("tables", false, "List tables")

	TIME_FORMAT = flag.String("time-format", "", "time format to use")
	NO_RECYCLE_MEM = flag.Bool("no-recycle-mem", false, "don't recycle memory slabs (use Go GC instead)")

	FLAGS.CACHED_QUERIES = flag.Bool("cache-queries", false, "Cache query results per block")

}

func RunQueryCmdLine() {
	addQueryFlags()
	flag.Parse()

	if *LIST_TABLES {
		printer.PrintTables()
		return
	}

	if *TIME_FORMAT != "" {
		OPTS.TIME_FORMAT = GetTimeFormat(*TIME_FORMAT)
	}

	table := *FLAGS.TABLE
	if table == "" {
		flag.PrintDefaults()
		return
	}

	t := GetTable(table)
	if IsNotExist(t) {
		Error(t.Name, "table can not be loaded or does not exist in", *FLAGS.DIR)
	}

	ints := make([]string, 0)
	groups := make([]string, 0)
	strs := make([]string, 0)

	if *FLAGS.GROUPS != "" {
		groups = strings.Split(*FLAGS.GROUPS, *FLAGS.FIELD_SEPARATOR)
		OPTS.GROUP_BY = groups

	}

	if *FLAGS.LUAFILE != "" {
		luajit.SetLuaScript(*FLAGS.LUAFILE)
	}

	if *NO_RECYCLE_MEM == true {
		FLAGS.RECYCLE_MEM = &FALSE
	}

	// PROCESS CMD LINE ARGS THAT USE COMMA DELIMITERS
	if *FLAGS.STRS != "" {
		strs = strings.Split(*FLAGS.STRS, *FLAGS.FIELD_SEPARATOR)
	}
	if *FLAGS.INTS != "" {
		ints = strings.Split(*FLAGS.INTS, *FLAGS.FIELD_SEPARATOR)
	}
	if *FLAGS.PROFILE && PROFILER_ENABLED {
		profile := RUN_PROFILER()
		defer profile.Start().Stop()
	}

	if *FLAGS.LOAD_THEN_QUERY {
		FLAGS.LOAD_AND_QUERY = &FALSE
	}

	if *FLAGS.READ_ROWSTORE {
		FLAGS.READ_INGESTION_LOG = &TRUE
	}

	// LOAD TABLE INFOS BEFORE WE CREATE OUR FILTERS, SO WE CAN CREATE FILTERS ON
	// THE RIGHT COLUMN ID
	md_io.LoadTableInfo(t)
	LoadRecords(t, nil)

	count := 0
	for _, block := range t.BlockList {
		count += int(block.Info.NumRecords)
	}

	Debug("WILL INSPECT", count, "RECORDS")

	groupings := []specs.Grouping{}
	for _, g := range groups {
		groupings = append(groupings, specs.GroupingForTable(t, g))
	}

	aggs := []specs.Aggregation{}
	for _, agg := range ints {
		aggs = append(aggs, specs.AggregationForTable(t, agg, *FLAGS.OP))
	}

	// VERIFY THE KEY TABLE IS IN ORDER, OTHERWISE WE NEED TO EXIT
	Debug("KEY TABLE", t.KeyTable)
	Debug("KEY TYPES", t.KeyTypes)

	used := make(map[int16]int)
	for _, v := range t.KeyTable {
		used[v]++
		if used[v] > 1 {
			Error("THERE IS A SERIOUS KEY TABLE INCONSISTENCY")
			return
		}
	}

	loadSpec := specs.NewTableLoadSpec(t)
	filterSpec := filters.FilterSpec{Int: *FLAGS.INT_FILTERS, Str: *FLAGS.STR_FILTERS, Set: *FLAGS.SET_FILTERS}
	filters := filters.BuildFilters(t, &loadSpec, filterSpec)

	query_params := specs.QueryParams{Groups: groupings, Filters: filters, Aggregations: aggs}
	querySpec := specs.QuerySpec{QueryParams: query_params}

	for _, v := range groups {
		switch md.GetColumnType(t, v) {
		case STR_VAL:
			loadSpec.Str(v)
		case INT_VAL:
			loadSpec.Int(v)
		default:
			printer.PrintColInfo(t)
			fmt.Println("")
			Error("Unknown column type for column: ", v, md.GetColumnType(t, v))
		}

	}
	for _, v := range strs {
		loadSpec.Str(v)
	}
	for _, v := range ints {
		loadSpec.Int(v)
	}

	if *FLAGS.SORT != "" {
		if *FLAGS.SORT != OPTS.SORT_COUNT {
			loadSpec.Int(*FLAGS.SORT)
		}
		querySpec.OrderBy = *FLAGS.SORT
	} else {
		querySpec.OrderBy = ""
	}

	if *FLAGS.TIME {
		// TODO: infer the TimeBucket size
		querySpec.TimeBucket = *FLAGS.TIME_BUCKET
		Debug("USING TIME BUCKET", querySpec.TimeBucket, "SECONDS")
		loadSpec.Int(*FLAGS.TIME_COL)
		time_col_id, ok := t.KeyTable[*FLAGS.TIME_COL]
		if ok {
			OPTS.TIME_COL_ID = time_col_id
		}
	}

	if *FLAGS.WEIGHT_COL != "" {
		OPTS.WEIGHT_COL = true
		loadSpec.Int(*FLAGS.WEIGHT_COL)
		OPTS.WEIGHT_COL_ID = t.KeyTable[*FLAGS.WEIGHT_COL]
	}

	querySpec.Limit = int16(*FLAGS.LIMIT)

	if *FLAGS.SAMPLES {
		OPTS.HOLD_MATCHES = true
		OPTS.DELETE_BLOCKS_AFTER_QUERY = false

		loadSpec := specs.NewTableLoadSpec(t)
		loadSpec.LoadAllColumns = true

		LoadAndQueryRecords(t, &loadSpec, &querySpec)

		printer.PrintSamples(t)

		return
	}

	if *FLAGS.EXPORT {
		loadSpec.LoadAllColumns = true
	}

	if !*FLAGS.PRINT_INFO {
		// DISABLE GC FOR QUERY PATH
		Debug("ADDING BULLET HOLES FOR SPEED (DISABLING GC)")
		debug.SetGCPercent(-1)

		Debug("USING LOAD SPEC", loadSpec)

		Debug("USING QUERY SPEC", querySpec)

		start := time.Now()
		// We can load and query at the same time
		if *FLAGS.LOAD_AND_QUERY {
			count = LoadAndQueryRecords(t, &loadSpec, &querySpec)

			end := time.Now()
			Debug("LOAD AND QUERY RECORDS TOOK", end.Sub(start))
			printer.PrintFinalResults(&querySpec)

			if FLAGS.ANOVA_ICC != nil && *FLAGS.ANOVA_ICC {
				CalculateICC(&querySpec)
			}
		}

	}

	if *FLAGS.EXPORT {
		Print("EXPORTED RECORDS TO", path.Join(t.Name, "export"))
	}

	if *FLAGS.PRINT_INFO {
		t := GetTable(table)
		FLAGS.LOAD_AND_QUERY = &FALSE

		LoadRecords(t, nil)
		printer.PrintColInfo(t)
	}

}
