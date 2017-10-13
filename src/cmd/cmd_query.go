package sybilCmd

import sybil "github.com/logv/sybil/src/lib"

import "fmt"
import "flag"
import "strings"
import "time"
import "path"
import "runtime/debug"

var MaxRecords_NoGc = 4 * 1000 * 1000 // 4 million

var ListTables *bool
var TimeFormat *string
var NoRecycleMem *bool

func addQueryFlags() {

	sybil.FLAGS.PrintInfo = flag.Bool("info", false, "Print table info")
	sybil.FLAGS.SORT = flag.String("sort", sybil.OPTS.SortCount, "Int Column to sort by")
	sybil.FLAGS.LIMIT = flag.Int("limit", 100, "Number of results to return")

	sybil.FLAGS.TIME = flag.Bool("time", false, "make a time rollup")
	sybil.FLAGS.TimeCol = flag.String("time-col", "time", "which column to treat as a timestamp (use with -time flag)")
	sybil.FLAGS.TimeBucket = flag.Int("time-bucket", 60*60, "time bucket (in seconds)")
	sybil.FLAGS.WeightCol = flag.String("weight-col", "", "Which column to treat as an optional weighting column")

	sybil.FLAGS.OP = flag.String("op", "avg", "metric to calculate, either 'avg' or 'hist'")
	sybil.FLAGS.LogHist = flag.Bool("loghist", false, "Use nested logarithmic histograms")
	if sybil.EnableHdr {
		sybil.FLAGS.HdrHist = flag.Bool("hdr", false, "Use HDR Histograms (can be slow)")
	}

	sybil.FLAGS.PRINT = flag.Bool("print", true, "Print some records")
	sybil.FLAGS.SAMPLES = flag.Bool("samples", false, "Grab samples")
	sybil.FLAGS.IntFilters = flag.String("int-filter", "", "Int filters, format: col:op:val")

	sybil.FLAGS.HistBucket = flag.Int("int-bucket", 0, "Int hist bucket size")

	sybil.FLAGS.StrReplace = flag.String("str-replace", "", "Str replacement, format: col:find:replace")
	sybil.FLAGS.StrFilters = flag.String("str-filter", "", "Str filters, format: col:op:val")
	sybil.FLAGS.SetFilters = flag.String("set-filter", "", "Set filters, format: col:op:val")
	sybil.FLAGS.UpdateTableInfo = flag.Bool("update-info", false, "Re-compute cached column data")

	sybil.FLAGS.INTS = flag.String("int", "", "Integer values to aggregate")
	sybil.FLAGS.STRS = flag.String("str", "", "String values to load")
	sybil.FLAGS.GROUPS = flag.String("group", "", "values group by")

	sybil.FLAGS.EXPORT = flag.Bool("export", false, "export data to TSV")

	sybil.FLAGS.ReadRowstore = flag.Bool("read-log", false, "read the ingestion log (can take longer!)")

	sybil.FLAGS.JSON = flag.Bool("json", false, "Print results in JSON format")
	sybil.FLAGS.AnovaIcc = flag.Bool("icc", false, "Calculate intraclass co-efficient (ANOVA)")

	if sybil.EnableLua {
		sybil.FLAGS.LUAFILE = flag.String("lua", "", "Script to execute with lua map reduce")
	}

	ListTables = flag.Bool("tables", false, "List tables")

	TimeFormat = flag.String("time-format", "", "time format to use")
	NoRecycleMem = flag.Bool("no-recycle-mem", false, "don't recycle memory slabs (use Go GC instead)")

	sybil.FLAGS.CachedQueries = flag.Bool("cache-queries", false, "Cache query results per block")

}

func RunQueryCmdLine() {
	addQueryFlags()
	flag.Parse()

	if *ListTables {
		sybil.PrintTables()
		return
	}

	if *TimeFormat != "" {
		sybil.OPTS.TimeFormat = sybil.GetTimeFormat(*TimeFormat)
	}

	table := *sybil.FLAGS.Table
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
		groups = strings.Split(*sybil.FLAGS.GROUPS, *sybil.FLAGS.FieldSeparator)
		sybil.OPTS.GroupBy = groups

	}

	if *sybil.FLAGS.LUAFILE != "" {
		sybil.SetLuaScript(*sybil.FLAGS.LUAFILE)
	}

	if *NoRecycleMem == true {
		sybil.FLAGS.RecycleMem = &sybil.FALSE
	}

	// PROCESS CMD LINE ARGS THAT USE COMMA DELIMITERS
	if *sybil.FLAGS.STRS != "" {
		strs = strings.Split(*sybil.FLAGS.STRS, *sybil.FLAGS.FieldSeparator)
	}
	if *sybil.FLAGS.INTS != "" {
		ints = strings.Split(*sybil.FLAGS.INTS, *sybil.FLAGS.FieldSeparator)
	}
	if *sybil.FLAGS.Profile && sybil.ProfilerEnabled {
		profile := sybil.RunProfiler()
		defer profile.Start().Stop()
	}

	if *sybil.FLAGS.LoadThenQuery {
		sybil.FLAGS.LoadAndQuery = &FALSE
	}

	if *sybil.FLAGS.ReadRowstore {
		sybil.FLAGS.ReadIngestionLog = &TRUE
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
	filterSpec := sybil.FilterSpec{Int: *sybil.FLAGS.IntFilters, Str: *sybil.FLAGS.StrFilters, Set: *sybil.FLAGS.SetFilters}
	filters := sybil.BuildFilters(t, &loadSpec, filterSpec)

	queryParams := sybil.QueryParams{Groups: groupings, Filters: filters, Aggregations: aggs}
	querySpec := sybil.QuerySpec{QueryParams: queryParams}

	for _, v := range groups {
		switch t.GetColumnType(v) {
		case sybil.StrVal:
			loadSpec.Str(v)
		case sybil.IntVal:
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
		if *sybil.FLAGS.SORT != sybil.OPTS.SortCount {
			loadSpec.Int(*sybil.FLAGS.SORT)
		}
		querySpec.OrderBy = *sybil.FLAGS.SORT
	} else {
		querySpec.OrderBy = ""
	}

	if *sybil.FLAGS.TIME {
		// TODO: infer the TimeBucket size
		querySpec.TimeBucket = *sybil.FLAGS.TimeBucket
		sybil.Debug("USING TIME BUCKET", querySpec.TimeBucket, "SECONDS")
		loadSpec.Int(*sybil.FLAGS.TimeCol)
		timeColId, ok := t.KeyTable[*sybil.FLAGS.TimeCol]
		if ok {
			sybil.OPTS.TimeColId = timeColId
		}
	}

	if *sybil.FLAGS.WeightCol != "" {
		sybil.OPTS.WeightCol = true
		loadSpec.Int(*sybil.FLAGS.WeightCol)
		sybil.OPTS.WeightColId = t.KeyTable[*sybil.FLAGS.WeightCol]
	}

	querySpec.Limit = int16(*sybil.FLAGS.LIMIT)

	if *sybil.FLAGS.SAMPLES {
		sybil.HoldMatches = true
		sybil.DeleteBlocksAfterQuery = false

		loadSpec := t.NewLoadSpec()
		loadSpec.LoadAllColumns = true

		t.LoadAndQueryRecords(&loadSpec, &querySpec)

		t.PrintSamples()

		return
	}

	if *sybil.FLAGS.EXPORT {
		loadSpec.LoadAllColumns = true
	}

	if !*sybil.FLAGS.PrintInfo {
		// DISABLE GC FOR QUERY PATH
		sybil.Debug("ADDING BULLET HOLES FOR SPEED (DISABLING GC)")
		debug.SetGCPercent(-1)

		sybil.Debug("USING LOAD SPEC", loadSpec)

		sybil.Debug("USING QUERY SPEC", querySpec)

		start := time.Now()
		// We can load and query at the same time
		if *sybil.FLAGS.LoadAndQuery {
			count = t.LoadAndQueryRecords(&loadSpec, &querySpec)

			end := time.Now()
			sybil.Debug("LOAD AND QUERY RECORDS TOOK", end.Sub(start))
			querySpec.PrintResults()

			if sybil.FLAGS.AnovaIcc != nil && *sybil.FLAGS.AnovaIcc {
				querySpec.CalculateICC()
			}
		}

	}

	if *sybil.FLAGS.EXPORT {
		sybil.Print("EXPORTED RECORDS TO", path.Join(t.Name, "export"))
	}

	if *sybil.FLAGS.PrintInfo {
		t := sybil.GetTable(table)
		sybil.FLAGS.LoadAndQuery = &FALSE

		t.LoadRecords(nil)
		t.PrintColInfo()
	}

}
