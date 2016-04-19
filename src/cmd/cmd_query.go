package sybil_cmd

import sybil "github.com/logV/sybil/src/lib"

import "log"
import "fmt"
import "flag"
import "strings"
import "time"
import "runtime/debug"

var MAX_RECORDS_NO_GC = 4 * 1000 * 1000 // 4 million

var LIST_TABLES *bool

func addQueryFlags() {

	sybil.FLAGS.TIME = flag.Bool("time", false, "make a time rollup")
	sybil.FLAGS.TIME_COL = flag.String("time-col", "time", "which column to treat as a timestamp (use with -time flag)")
	sybil.FLAGS.TIME_BUCKET = flag.Int("time-bucket", 60*60, "time bucket (in seconds)")
	sybil.FLAGS.WEIGHT_COL = flag.String("weight-col", "", "Which column to treat as an optional weighting column")

	sybil.FLAGS.OP = flag.String("op", "avg", "metric to calculate, either 'avg' or 'hist'")
	sybil.FLAGS.PRINT = flag.Bool("print", false, "Print some records")
	sybil.FLAGS.SAMPLES = flag.Bool("samples", false, "Grab samples")
	sybil.FLAGS.INT_FILTERS = flag.String("int-filter", "", "Int filters, format: col:op:val")

	sybil.FLAGS.HIST_BUCKET = flag.Int("int-bucket", 0, "Int hist bucket size")

	sybil.FLAGS.STR_REPLACE = flag.String("str-replace", "", "Str replacement, format: col:find:replace")
	sybil.FLAGS.STR_FILTERS = flag.String("str-filter", "", "Str filters, format: col:op:val")
	sybil.FLAGS.SET_FILTERS = flag.String("set-filter", "", "Set filters, format: col:op:val")
	LIST_TABLES = flag.Bool("tables", false, "List tables")
	sybil.FLAGS.UPDATE_TABLE_INFO = flag.Bool("update-info", false, "Re-compute cached column data")

	sybil.FLAGS.INTS = flag.String("int", "", "Integer values to aggregate")
	sybil.FLAGS.STRS = flag.String("str", "", "String values to load")
	sybil.FLAGS.GROUPS = flag.String("group", "", "values group by")

	sybil.FLAGS.SKIP_ROWSTORE = flag.Bool("no-read-log", false, "skip reading the ingestion log")

	sybil.FLAGS.JSON = flag.Bool("json", false, "Print results in JSON format")
	sybil.FLAGS.ANOVA_ICC = flag.Bool("icc", false, "Calculate intraclass co-efficient (ANOVA)")

}

func RunQueryCmdLine() {
	addQueryFlags()
	flag.Parse()

	if *LIST_TABLES {
		sybil.PrintTables()
		return
	}

	table := *sybil.FLAGS.TABLE
	if table == "" {
		flag.PrintDefaults()
		return
	}
	t := sybil.GetTable(table)

	ints := make([]string, 0)
	groups := make([]string, 0)
	strs := make([]string, 0)

	if *sybil.FLAGS.GROUPS != "" {
		groups = strings.Split(*sybil.FLAGS.GROUPS, ",")
		sybil.OPTS.GROUP_BY = groups

	}

	// PROCESS CMD LINE ARGS THAT USE COMMA DELIMITERS
	if *sybil.FLAGS.STRS != "" {
		strs = strings.Split(*sybil.FLAGS.STRS, ",")
	}
	if *sybil.FLAGS.INTS != "" {
		ints = strings.Split(*sybil.FLAGS.INTS, ",")
	}
	if *sybil.FLAGS.PROFILE && sybil.PROFILER_ENABLED {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	if *sybil.FLAGS.LOAD_THEN_QUERY {
		sybil.FLAGS.LOAD_AND_QUERY = &FALSE
	}

	if *sybil.FLAGS.SKIP_ROWSTORE {
		sybil.FLAGS.READ_INGESTION_LOG = &FALSE
	}

	// LOAD TABLE INFOS BEFORE WE CREATE OUR FILTERS, SO WE CAN CREATE FILTERS ON
	// THE RIGHT COLUMN ID
	t.LoadTableInfo()
	t.LoadRecords(nil)

	count := 0
	for _, block := range t.BlockList {
		count += int(block.Info.NumRecords)
	}

	log.Println("WILL INSPECT", count, "RECORDS")

	groupings := []sybil.Grouping{}
	for _, g := range groups {
		groupings = append(groupings, t.Grouping(g))
	}

	aggs := []sybil.Aggregation{}
	for _, agg := range ints {
		aggs = append(aggs, t.Aggregation(agg, *sybil.FLAGS.OP))
	}

	// VERIFY THE KEY TABLE IS IN ORDER, OTHERWISE WE NEED TO EXIT
	log.Println("KEY TABLE", t.KeyTable)
	log.Println("KEY TYPES", t.KeyTypes)

	used := make(map[int16]int)
	for _, v := range t.KeyTable {
		used[v]++
		if used[v] > 1 {
			log.Fatal("THERE IS A SERIOUS KEY TABLE INCONSISTENCY")
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
			log.Fatal("Unknown column type for column: ", v, t.GetColumnType(v))
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
		log.Println("USING TIME BUCKET", querySpec.TimeBucket, "SECONDS")
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

	if !*sybil.FLAGS.PRINT_INFO {
		// DISABLE GC FOR QUERY PATH
		log.Println("ADDING BULLET HOLES FOR SPEED (DISABLING GC)")
		debug.SetGCPercent(-1)

		log.Println("USING LOAD SPEC", loadSpec)

		log.Println("USING QUERY SPEC", querySpec)

		start := time.Now()
		// We can load and query at the same time
		if *sybil.FLAGS.LOAD_AND_QUERY {
			count = t.LoadAndQueryRecords(&loadSpec, &querySpec)

			end := time.Now()
			log.Println("LOAD AND QUERY RECORDS TOOK", end.Sub(start))
			querySpec.PrintResults()

			if sybil.FLAGS.ANOVA_ICC != nil && *sybil.FLAGS.ANOVA_ICC {
				querySpec.CalculateICC()
			}
		}

	}

	if *sybil.FLAGS.PRINT_INFO {
		t := sybil.GetTable(table)
		sybil.FLAGS.LOAD_AND_QUERY = &FALSE

		t.LoadRecords(nil)
		t.PrintColInfo()
	}

}
