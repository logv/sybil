package cmd

import (
	"flag"
	"fmt"
	"path"
	"runtime/debug"
	"strings"
	"time"

	sybil "github.com/logv/sybil/src/lib"
	"github.com/logv/sybil/src/lib/common"
	"github.com/logv/sybil/src/lib/config"
)

var MAX_RECORDS_NO_GC = 4 * 1000 * 1000 // 4 million

var LIST_TABLES *bool
var TIME_FORMAT *string
var NO_RECYCLE_MEM *bool

func addQueryFlags() {

	config.FLAGS.PRINT_INFO = flag.Bool("info", false, "Print table info")
	config.FLAGS.SORT = flag.String("sort", config.OPTS.SORT_COUNT, "Int Column to sort by")
	config.FLAGS.LIMIT = flag.Int("limit", 100, "Number of results to return")

	config.FLAGS.TIME = flag.Bool("time", false, "make a time rollup")
	config.FLAGS.TIME_COL = flag.String("time-col", "time", "which column to treat as a timestamp (use with -time flag)")
	config.FLAGS.TIME_BUCKET = flag.Int("time-bucket", 60*60, "time bucket (in seconds)")
	config.FLAGS.WEIGHT_COL = flag.String("weight-col", "", "Which column to treat as an optional weighting column")

	config.FLAGS.OP = flag.String("op", "avg", "metric to calculate, either 'avg' or 'hist'")
	config.FLAGS.LOG_HIST = flag.Bool("loghist", false, "Use nested logarithmic histograms")
	if sybil.ENABLE_HDR {
		config.FLAGS.HDR_HIST = flag.Bool("hdr", false, "Use HDR Histograms (can be slow)")
	}

	config.FLAGS.PRINT = flag.Bool("print", true, "Print some records")
	config.FLAGS.SAMPLES = flag.Bool("samples", false, "Grab samples")
	config.FLAGS.INT_FILTERS = flag.String("int-filter", "", "Int filters, format: col:op:val")

	config.FLAGS.HIST_BUCKET = flag.Int("int-bucket", 0, "Int hist bucket size")

	config.FLAGS.STR_REPLACE = flag.String("str-replace", "", "Str replacement, format: col:find:replace")
	config.FLAGS.STR_FILTERS = flag.String("str-filter", "", "Str filters, format: col:op:val")
	config.FLAGS.SET_FILTERS = flag.String("set-filter", "", "Set filters, format: col:op:val")
	config.FLAGS.UPDATE_TABLE_INFO = flag.Bool("update-info", false, "Re-compute cached column data")

	config.FLAGS.INTS = flag.String("int", "", "Integer values to aggregate")
	config.FLAGS.STRS = flag.String("str", "", "String values to load")
	config.FLAGS.GROUPS = flag.String("group", "", "values group by")

	config.FLAGS.EXPORT = flag.Bool("export", false, "export data to TSV")

	config.FLAGS.READ_ROWSTORE = flag.Bool("read-log", false, "read the ingestion log (can take longer!)")

	config.FLAGS.JSON = flag.Bool("json", false, "Print results in JSON format")
	config.FLAGS.ANOVA_ICC = flag.Bool("icc", false, "Calculate intraclass co-efficient (ANOVA)")

	if config.ENABLE_LUA {
		config.FLAGS.LUAFILE = flag.String("lua", "", "Script to execute with lua map reduce")
	}

	LIST_TABLES = flag.Bool("tables", false, "List tables")

	TIME_FORMAT = flag.String("time-format", "", "time format to use")
	NO_RECYCLE_MEM = flag.Bool("no-recycle-mem", false, "don't recycle memory slabs (use Go GC instead)")

	config.FLAGS.CACHED_QUERIES = flag.Bool("cache-queries", false, "Cache query results per block")

}

func RunQueryCmdLine() {
	addQueryFlags()
	flag.Parse()

	if *LIST_TABLES {
		sybil.PrintTables()
		return
	}

	if *TIME_FORMAT != "" {
		config.OPTS.TIME_FORMAT = sybil.GetTimeFormat(*TIME_FORMAT)
	}

	table := *config.FLAGS.TABLE
	if table == "" {
		flag.PrintDefaults()
		return
	}

	t := sybil.GetTable(table)
	if t.IsNotExist() {
		common.Error(t.Name, "table can not be loaded or does not exist in", *config.FLAGS.DIR)
	}

	ints := make([]string, 0)
	groups := make([]string, 0)
	strs := make([]string, 0)

	if *config.FLAGS.GROUPS != "" {
		groups = strings.Split(*config.FLAGS.GROUPS, *config.FLAGS.FIELD_SEPARATOR)
		config.OPTS.GROUP_BY = groups

	}

	if *config.FLAGS.LUAFILE != "" {
		sybil.SetLuaScript(*config.FLAGS.LUAFILE)
	}

	if *NO_RECYCLE_MEM == true {
		config.FLAGS.RECYCLE_MEM = &config.FALSE
	}

	// PROCESS CMD LINE ARGS THAT USE COMMA DELIMITERS
	if *config.FLAGS.STRS != "" {
		strs = strings.Split(*config.FLAGS.STRS, *config.FLAGS.FIELD_SEPARATOR)
	}
	if *config.FLAGS.INTS != "" {
		ints = strings.Split(*config.FLAGS.INTS, *config.FLAGS.FIELD_SEPARATOR)
	}
	if *config.FLAGS.PROFILE && config.PROFILER_ENABLED {
		profile := config.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	if *config.FLAGS.LOAD_THEN_QUERY {
		config.FLAGS.LOAD_AND_QUERY = &FALSE
	}

	if *config.FLAGS.READ_ROWSTORE {
		config.FLAGS.READ_INGESTION_LOG = &config.TRUE
	}

	// LOAD TABLE INFOS BEFORE WE CREATE OUR FILTERS, SO WE CAN CREATE FILTERS ON
	// THE RIGHT COLUMN ID
	t.LoadTableInfo()
	t.LoadRecords(nil)

	count := 0
	for _, block := range t.BlockList {
		count += int(block.Info.NumRecords)
	}

	common.Debug("WILL INSPECT", count, "RECORDS")

	groupings := []sybil.Grouping{}
	for _, g := range groups {
		groupings = append(groupings, t.Grouping(g))
	}

	aggs := []sybil.Aggregation{}
	for _, agg := range ints {
		aggs = append(aggs, t.Aggregation(agg, *config.FLAGS.OP))
	}

	// VERIFY THE KEY TABLE IS IN ORDER, OTHERWISE WE NEED TO EXIT
	common.Debug("KEY TABLE", t.KeyTable)
	common.Debug("KEY TYPES", t.KeyTypes)

	used := make(map[int16]int)
	for _, v := range t.KeyTable {
		used[v]++
		if used[v] > 1 {
			common.Error("THERE IS A SERIOUS KEY TABLE INCONSISTENCY")
			return
		}
	}

	loadSpec := t.NewLoadSpec()
	filterSpec := sybil.FilterSpec{Int: *config.FLAGS.INT_FILTERS, Str: *config.FLAGS.STR_FILTERS, Set: *config.FLAGS.SET_FILTERS}
	filters := sybil.BuildFilters(t, &loadSpec, filterSpec)

	query_params := sybil.QueryParams{Groups: groupings, Filters: filters, Aggregations: aggs}
	querySpec := sybil.QuerySpec{QueryParams: query_params}

	for _, v := range groups {
		switch t.GetColumnType(v) {
		case sybil.STR_VAL:
			loadSpec.Str(v)
		case sybil.INT_VAL:
			loadSpec.Int(v)
		default:
			t.PrintColInfo()
			fmt.Println("")
			common.Error("Unknown column type for column: ", v, t.GetColumnType(v))
		}

	}
	for _, v := range strs {
		loadSpec.Str(v)
	}
	for _, v := range ints {
		loadSpec.Int(v)
	}

	if *config.FLAGS.SORT != "" {
		if *config.FLAGS.SORT != config.OPTS.SORT_COUNT {
			loadSpec.Int(*config.FLAGS.SORT)
		}
		querySpec.OrderBy = *config.FLAGS.SORT
	} else {
		querySpec.OrderBy = ""
	}

	if *config.FLAGS.TIME {
		// TODO: infer the TimeBucket size
		querySpec.TimeBucket = *config.FLAGS.TIME_BUCKET
		common.Debug("USING TIME BUCKET", querySpec.TimeBucket, "SECONDS")
		loadSpec.Int(*config.FLAGS.TIME_COL)
		time_col_id, ok := t.KeyTable[*config.FLAGS.TIME_COL]
		if ok {
			config.OPTS.TIME_COL_ID = time_col_id
		}
	}

	if *config.FLAGS.WEIGHT_COL != "" {
		config.OPTS.WEIGHT_COL = true
		loadSpec.Int(*config.FLAGS.WEIGHT_COL)
		config.OPTS.WEIGHT_COL_ID = t.KeyTable[*config.FLAGS.WEIGHT_COL]
	}

	querySpec.Limit = int16(*config.FLAGS.LIMIT)

	if *config.FLAGS.SAMPLES {
		sybil.HOLD_MATCHES = true
		sybil.DELETE_BLOCKS_AFTER_QUERY = false

		loadSpec := t.NewLoadSpec()
		loadSpec.LoadAllColumns = true

		t.LoadAndQueryRecords(&loadSpec, &querySpec)

		t.PrintSamples()

		return
	}

	if *config.FLAGS.EXPORT {
		loadSpec.LoadAllColumns = true
	}

	if !*config.FLAGS.PRINT_INFO {
		// DISABLE GC FOR QUERY PATH
		common.Debug("ADDING BULLET HOLES FOR SPEED (DISABLING GC)")
		debug.SetGCPercent(-1)

		common.Debug("USING LOAD SPEC", loadSpec)

		common.Debug("USING QUERY SPEC", querySpec)

		start := time.Now()
		// We can load and query at the same time
		if *config.FLAGS.LOAD_AND_QUERY {
			count = t.LoadAndQueryRecords(&loadSpec, &querySpec)

			end := time.Now()
			common.Debug("LOAD AND QUERY RECORDS TOOK", end.Sub(start))
			querySpec.PrintResults()

			if config.FLAGS.ANOVA_ICC != nil && *config.FLAGS.ANOVA_ICC {
				querySpec.CalculateICC()
			}
		}

	}

	if *config.FLAGS.EXPORT {
		common.Print("EXPORTED RECORDS TO", path.Join(t.Name, "export"))
	}

	if *config.FLAGS.PRINT_INFO {
		t := sybil.GetTable(table)
		config.FLAGS.LOAD_AND_QUERY = &FALSE

		t.LoadRecords(nil)
		t.PrintColInfo()
	}

}
