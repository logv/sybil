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
)

var MAX_RECORDS_NO_GC = 4 * 1000 * 1000 // 4 million

var LIST_TABLES *bool
var TIME_FORMAT *string
var NO_RECYCLE_MEM *bool

func addQueryFlags() {

	common.FLAGS.PRINT_INFO = flag.Bool("info", false, "Print table info")
	common.FLAGS.SORT = flag.String("sort", common.OPTS.SORT_COUNT, "Int Column to sort by")
	common.FLAGS.LIMIT = flag.Int("limit", 100, "Number of results to return")

	common.FLAGS.TIME = flag.Bool("time", false, "make a time rollup")
	common.FLAGS.TIME_COL = flag.String("time-col", "time", "which column to treat as a timestamp (use with -time flag)")
	common.FLAGS.TIME_BUCKET = flag.Int("time-bucket", 60*60, "time bucket (in seconds)")
	common.FLAGS.WEIGHT_COL = flag.String("weight-col", "", "Which column to treat as an optional weighting column")

	common.FLAGS.OP = flag.String("op", "avg", "metric to calculate, either 'avg' or 'hist'")
	common.FLAGS.LOG_HIST = flag.Bool("loghist", false, "Use nested logarithmic histograms")
	if sybil.ENABLE_HDR {
		common.FLAGS.HDR_HIST = flag.Bool("hdr", false, "Use HDR Histograms (can be slow)")
	}

	common.FLAGS.PRINT = flag.Bool("print", true, "Print some records")
	common.FLAGS.SAMPLES = flag.Bool("samples", false, "Grab samples")
	common.FLAGS.INT_FILTERS = flag.String("int-filter", "", "Int filters, format: col:op:val")

	common.FLAGS.HIST_BUCKET = flag.Int("int-bucket", 0, "Int hist bucket size")

	common.FLAGS.STR_REPLACE = flag.String("str-replace", "", "Str replacement, format: col:find:replace")
	common.FLAGS.STR_FILTERS = flag.String("str-filter", "", "Str filters, format: col:op:val")
	common.FLAGS.SET_FILTERS = flag.String("set-filter", "", "Set filters, format: col:op:val")
	common.FLAGS.UPDATE_TABLE_INFO = flag.Bool("update-info", false, "Re-compute cached column data")

	common.FLAGS.INTS = flag.String("int", "", "Integer values to aggregate")
	common.FLAGS.STRS = flag.String("str", "", "String values to load")
	common.FLAGS.GROUPS = flag.String("group", "", "values group by")

	common.FLAGS.EXPORT = flag.Bool("export", false, "export data to TSV")

	common.FLAGS.READ_ROWSTORE = flag.Bool("read-log", false, "read the ingestion log (can take longer!)")

	common.FLAGS.JSON = flag.Bool("json", false, "Print results in JSON format")
	common.FLAGS.ANOVA_ICC = flag.Bool("icc", false, "Calculate intraclass co-efficient (ANOVA)")

	if common.ENABLE_LUA {
		common.FLAGS.LUAFILE = flag.String("lua", "", "Script to execute with lua map reduce")
	}

	LIST_TABLES = flag.Bool("tables", false, "List tables")

	TIME_FORMAT = flag.String("time-format", "", "time format to use")
	NO_RECYCLE_MEM = flag.Bool("no-recycle-mem", false, "don't recycle memory slabs (use Go GC instead)")

	common.FLAGS.CACHED_QUERIES = flag.Bool("cache-queries", false, "Cache query results per block")

}

func RunQueryCmdLine() {
	addQueryFlags()
	flag.Parse()

	if *LIST_TABLES {
		sybil.PrintTables()
		return
	}

	if *TIME_FORMAT != "" {
		common.OPTS.TIME_FORMAT = sybil.GetTimeFormat(*TIME_FORMAT)
	}

	table := *common.FLAGS.TABLE
	if table == "" {
		flag.PrintDefaults()
		return
	}

	t := sybil.GetTable(table)
	if t.IsNotExist() {
		common.Error(t.Name, "table can not be loaded or does not exist in", *common.FLAGS.DIR)
	}

	ints := make([]string, 0)
	groups := make([]string, 0)
	strs := make([]string, 0)

	if *common.FLAGS.GROUPS != "" {
		groups = strings.Split(*common.FLAGS.GROUPS, *common.FLAGS.FIELD_SEPARATOR)
		common.OPTS.GROUP_BY = groups

	}

	if *common.FLAGS.LUAFILE != "" {
		sybil.SetLuaScript(*common.FLAGS.LUAFILE)
	}

	if *NO_RECYCLE_MEM == true {
		common.FLAGS.RECYCLE_MEM = &common.FALSE
	}

	// PROCESS CMD LINE ARGS THAT USE COMMA DELIMITERS
	if *common.FLAGS.STRS != "" {
		strs = strings.Split(*common.FLAGS.STRS, *common.FLAGS.FIELD_SEPARATOR)
	}
	if *common.FLAGS.INTS != "" {
		ints = strings.Split(*common.FLAGS.INTS, *common.FLAGS.FIELD_SEPARATOR)
	}
	if *common.FLAGS.PROFILE && common.PROFILER_ENABLED {
		profile := common.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	if *common.FLAGS.LOAD_THEN_QUERY {
		common.FLAGS.LOAD_AND_QUERY = &FALSE
	}

	if *common.FLAGS.READ_ROWSTORE {
		common.FLAGS.READ_INGESTION_LOG = &TRUE
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
		aggs = append(aggs, t.Aggregation(agg, *common.FLAGS.OP))
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
	filterSpec := sybil.FilterSpec{Int: *common.FLAGS.INT_FILTERS, Str: *common.FLAGS.STR_FILTERS, Set: *common.FLAGS.SET_FILTERS}
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

	if *common.FLAGS.SORT != "" {
		if *common.FLAGS.SORT != common.OPTS.SORT_COUNT {
			loadSpec.Int(*common.FLAGS.SORT)
		}
		querySpec.OrderBy = *common.FLAGS.SORT
	} else {
		querySpec.OrderBy = ""
	}

	if *common.FLAGS.TIME {
		// TODO: infer the TimeBucket size
		querySpec.TimeBucket = *common.FLAGS.TIME_BUCKET
		common.Debug("USING TIME BUCKET", querySpec.TimeBucket, "SECONDS")
		loadSpec.Int(*common.FLAGS.TIME_COL)
		time_col_id, ok := t.KeyTable[*common.FLAGS.TIME_COL]
		if ok {
			common.OPTS.TIME_COL_ID = time_col_id
		}
	}

	if *common.FLAGS.WEIGHT_COL != "" {
		common.OPTS.WEIGHT_COL = true
		loadSpec.Int(*common.FLAGS.WEIGHT_COL)
		common.OPTS.WEIGHT_COL_ID = t.KeyTable[*common.FLAGS.WEIGHT_COL]
	}

	querySpec.Limit = int16(*common.FLAGS.LIMIT)

	if *common.FLAGS.SAMPLES {
		sybil.HOLD_MATCHES = true
		sybil.DELETE_BLOCKS_AFTER_QUERY = false

		loadSpec := t.NewLoadSpec()
		loadSpec.LoadAllColumns = true

		t.LoadAndQueryRecords(&loadSpec, &querySpec)

		t.PrintSamples()

		return
	}

	if *common.FLAGS.EXPORT {
		loadSpec.LoadAllColumns = true
	}

	if !*common.FLAGS.PRINT_INFO {
		// DISABLE GC FOR QUERY PATH
		common.Debug("ADDING BULLET HOLES FOR SPEED (DISABLING GC)")
		debug.SetGCPercent(-1)

		common.Debug("USING LOAD SPEC", loadSpec)

		common.Debug("USING QUERY SPEC", querySpec)

		start := time.Now()
		// We can load and query at the same time
		if *common.FLAGS.LOAD_AND_QUERY {
			count = t.LoadAndQueryRecords(&loadSpec, &querySpec)

			end := time.Now()
			common.Debug("LOAD AND QUERY RECORDS TOOK", end.Sub(start))
			querySpec.PrintResults()

			if common.FLAGS.ANOVA_ICC != nil && *common.FLAGS.ANOVA_ICC {
				querySpec.CalculateICC()
			}
		}

	}

	if *common.FLAGS.EXPORT {
		common.Print("EXPORTED RECORDS TO", path.Join(t.Name, "export"))
	}

	if *common.FLAGS.PRINT_INFO {
		t := sybil.GetTable(table)
		common.FLAGS.LOAD_AND_QUERY = &FALSE

		t.LoadRecords(nil)
		t.PrintColInfo()
	}

}
