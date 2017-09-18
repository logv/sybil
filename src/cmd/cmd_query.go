package sybil_cmd

import sybil "github.com/logv/sybil/src/lib"

import "fmt"
import "flag"
import "strings"
import "time"
import "path"
import "runtime/debug"

var MAX_RECORDS_NO_GC = 4 * 1000 * 1000 // 4 million

var LIST_TABLES *bool
var TIME_FORMAT *string
var NO_RECYCLE_MEM *bool

func addQueryFlags() {

	sybil.FLAGS.PRINT_INFO = flag.Bool("info", false, "Print table info")
	sybil.FLAGS.SORT = flag.String("sort", sybil.OPTS.SORT_COUNT, "Int Column to sort by")
	sybil.FLAGS.LIMIT = flag.Int("limit", 100, "Number of results to return")

	sybil.FLAGS.TIME = flag.Bool("time", false, "make a time rollup")
	sybil.FLAGS.TIME_COL = flag.String("time-col", "time", "which column to treat as a timestamp (use with -time flag)")
	sybil.FLAGS.TIME_BUCKET = flag.Int("time-bucket", 60*60, "time bucket (in seconds)")
	sybil.FLAGS.WEIGHT_COL = flag.String("weight-col", "", "Which column to treat as an optional weighting column")

	sybil.FLAGS.OP = flag.String("op", "avg", "metric to calculate, either 'avg' or 'hist'")
	sybil.FLAGS.LOG_HIST = flag.Bool("loghist", false, "Use nested logarithmic histograms")
	if sybil.ENABLE_HDR {
		sybil.FLAGS.HDR_HIST = flag.Bool("hdr", false, "Use HDR Histograms (can be slow)")
	}

	sybil.FLAGS.PRINT = flag.Bool("print", true, "Print some records")
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

	sybil.FLAGS.EXPORT = flag.Bool("export", false, "export data to TSV")

	sybil.FLAGS.READ_ROWSTORE = flag.Bool("read-log", false, "read the ingestion log (can take longer!)")

	sybil.FLAGS.JSON = flag.Bool("json", false, "Print results in JSON format")
	sybil.FLAGS.ANOVA_ICC = flag.Bool("icc", false, "Calculate intraclass co-efficient (ANOVA)")

	if sybil.ENABLE_LUA {
		sybil.FLAGS.LUAFILE = flag.String("lua", "", "Script to execute with lua map reduce")
	}

	LIST_TABLES = flag.Bool("tables", false, "List tables")

	TIME_FORMAT = flag.String("time-format", "", "time format to use")
	NO_RECYCLE_MEM = flag.Bool("no-recycle-mem", false, "don't recycle memory slabs (use Go GC instead)")

}

func RunQueryCmdLine() {
	addQueryFlags()
	flag.Parse()

	if *LIST_TABLES {
		sybil.PrintTables()
		return
	}

	if *TIME_FORMAT != "" {
		sybil.OPTS.TIME_FORMAT = sybil.GetTimeFormat(*TIME_FORMAT)
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

	if *sybil.FLAGS.GROUPS != "" {
		groups = strings.Split(*sybil.FLAGS.GROUPS, *sybil.FLAGS.FIELD_SEPARATOR)
		sybil.OPTS.GROUP_BY = groups

	}

	if *sybil.FLAGS.LUAFILE != "" {
		sybil.SetLuaScript(*sybil.FLAGS.LUAFILE)
	}

	if *NO_RECYCLE_MEM == true {
		sybil.FLAGS.RECYCLE_MEM = &sybil.FALSE
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

	if *sybil.FLAGS.LOAD_THEN_QUERY {
		sybil.FLAGS.LOAD_AND_QUERY = &FALSE
	}

	if *sybil.FLAGS.READ_ROWSTORE {
		sybil.FLAGS.READ_INGESTION_LOG = &TRUE
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

	querySpec := sybil.QuerySpec{Groups: groupings, Filters: filters, Aggregations: aggs}

	for _, v := range groups {
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

	if *sybil.FLAGS.TIME {
		// TODO: infer the TimeBucket size
		querySpec.TimeBucket = *sybil.FLAGS.TIME_BUCKET
		sybil.Debug("USING TIME BUCKET", querySpec.TimeBucket, "SECONDS")
		loadSpec.Int(*sybil.FLAGS.TIME_COL)
		time_col_id, ok := t.KeyTable[*sybil.FLAGS.TIME_COL]
		if ok {
			sybil.OPTS.TIME_COL_ID = time_col_id
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
			count = t.LoadAndQueryRecords(&loadSpec, &querySpec)

			end := time.Now()
			sybil.Debug("LOAD AND QUERY RECORDS TOOK", end.Sub(start))
			querySpec.PrintResults()

			if sybil.FLAGS.ANOVA_ICC != nil && *sybil.FLAGS.ANOVA_ICC {
				querySpec.CalculateICC()
			}
		}

	}

	if *sybil.FLAGS.EXPORT {
		sybil.Print("EXPORTED RECORDS TO", path.Join(t.Name, "export"))
	}

	if *sybil.FLAGS.PRINT_INFO {
		t := sybil.GetTable(table)
		sybil.FLAGS.LOAD_AND_QUERY = &FALSE

		t.LoadRecords(nil)
		t.PrintColInfo()
	}

}
