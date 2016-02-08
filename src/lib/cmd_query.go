package pcs

import "log"
import "fmt"
import "flag"
import "strings"
import "time"
import "strconv"
import "runtime/debug"

var MAX_RECORDS_NO_GC = 4 * 1000 * 1000 // 4 million

var SAMPLES = false
var f_SAMPLES *bool = &SAMPLES
var f_LIST_TABLES *bool

func queryTable(name string, loadSpec *LoadSpec, querySpec *QuerySpec) {
	table := GetTable(name)

	table.MatchAndAggregate(querySpec)

	querySpec.printResults()

	if *f_SESSION_COL != "" {
		start := time.Now()
		session_maps := SessionizeRecords(querySpec.Matched, *f_SESSION_COL)
		end := time.Now()
		log.Println("SESSIONIZED", len(querySpec.Matched), "RECORDS INTO", len(session_maps), "SESSIONS, TOOK", end.Sub(start))
	}
}

func addFlags() {

	f_TIME = flag.Bool("time", false, "do a time rollup!")
	f_TIME_COL = flag.String("time-col", "time", "which column to treat as a timestamp (use with -time flag)")
	f_OP = flag.String("op", "avg", "metric to calculate, either 'avg' or 'hist'")
	f_PRINT = flag.Bool("print", false, "Print some records")
	f_SAMPLES = flag.Bool("samples", false, "Grab samples")
	f_INT_FILTERS = flag.String("int-filter", "", "Int filters, format: col:op:val")
	f_STR_FILTERS = flag.String("str-filter", "", "Str filters, format: col:op:val")
	f_LIST_TABLES = flag.Bool("tables", false, "List tables")
	f_UPDATE_TABLE_INFO = flag.Bool("update-info", false, "Re-compute cached column data")

	f_SESSION_COL = flag.String("session", "", "Column to use for sessionizing")
	f_INTS = flag.String("int", "", "Integer values to aggregate")
	f_STRS = flag.String("str", "", "String values to load")
	f_GROUPS = flag.String("group", "", "values group by")

	f_LOAD_AND_QUERY = flag.Bool("laq", false, "Load and Query")
	f_PRINT_KEYS = flag.Bool("print-keys", false, "Print table key info")
	f_JSON = flag.Bool("json", false, "Print results in JSON format")
}

func RunQueryCmdLine() {
	addFlags()
	flag.Parse()

	if *f_LIST_TABLES {
		printTables()
		return
	}

	table := *f_TABLE
	if table == "" {
		flag.PrintDefaults()
		return
	}
	t := GetTable(table)

	ints := make([]string, 0)
	groups := make([]string, 0)
	strs := make([]string, 0)
	strfilters := make([]string, 0)
	intfilters := make([]string, 0)

	if *f_GROUPS != "" {
		groups = strings.Split(*f_GROUPS, ",")
		GROUP_BY = groups

	}

	// PROCESS CMD LINE ARGS THAT USE COMMA DELIMITERS
	if *f_STRS != "" {
		strs = strings.Split(*f_STRS, ",")
	}
	if *f_INTS != "" {
		ints = strings.Split(*f_INTS, ",")
	}
	if *f_INT_FILTERS != "" {
		intfilters = strings.Split(*f_INT_FILTERS, ",")
	}
	if *f_STR_FILTERS != "" {
		strfilters = strings.Split(*f_STR_FILTERS, ",")
	}

	if *f_PROFILE && PROFILER_ENABLED {
		profile := RUN_PROFILER()
		defer profile.Start().Stop()
	}

	// LOAD TABLE INFOS BEFORE WE CREATE OUR FILTERS, SO WE CAN CREATE FILTERS ON
	// THE RIGHT COLUMN ID
	t.LoadRecords(nil)

	count := 0
	for _, block := range t.BlockList {
		count += int(block.Info.NumRecords)
	}

	log.Println("WILL INSPECT", count, "RECORDS")

	if *f_SAMPLES {
		loadSpec := t.NewLoadSpec()
		loadSpec.LoadAllColumns = true

		// TODO: filter these records, too!
		t.LoadRecords(&loadSpec)

		t.printSamples()

		return
	}

	groupings := []Grouping{}
	for _, g := range groups {
		col_id := t.get_key_id(g)
		groupings = append(groupings, Grouping{g, col_id})
	}

	aggs := []Aggregation{}
	cached_table_info := true
	for _, agg := range ints {
		col_id := t.get_key_id(agg)
		aggs = append(aggs, t.Aggregation(agg, *f_OP))
		_, ok := t.IntInfo[col_id]
		if !ok {
			log.Println("MISSING CACHED INFO FOR", agg)
			cached_table_info = false
		}
	}

	// VERIFY THE KEY TABLE IS IN ORDER, OTHERWISE WE NEED TO EXIT
	if *f_PRINT_KEYS {
		log.Println("KEY TABLE", t.KeyTable)
		log.Println("KEY TYPES", t.KeyTypes)
	}

	used := make(map[int16]int)
	for _, v := range t.KeyTable {
		used[v]++
		if used[v] > 1 {
			log.Fatal("THERE IS A SERIOUS KEY TABLE INCONSISTENCY")
			return
		}
	}

	loadSpec := t.NewLoadSpec()
	filters := []Filter{}
	for _, filt := range intfilters {
		tokens := strings.Split(filt, ":")
		col := tokens[0]
		op := tokens[1]
		val, _ := strconv.ParseInt(tokens[2], 10, 64)

		filters = append(filters, t.IntFilter(col, op, int(val)))
		loadSpec.Int(col)
	}

	for _, filter := range strfilters {
		tokens := strings.Split(filter, ":")
		col := tokens[0]
		op := tokens[1]
		val := tokens[2]
		loadSpec.Str(col)

		filters = append(filters, t.StrFilter(col, op, val))

	}

	querySpec := QuerySpec{Groups: groupings, Filters: filters, Aggregations: aggs}
	querySpec.Punctuate()

	for _, v := range groups {
		loadSpec.Str(v)
	}
	for _, v := range strs {
		loadSpec.Str(v)
	}
	for _, v := range ints {
		loadSpec.Int(v)
	}

	if *f_SORT != "" {
		if *f_SORT != SORT_COUNT {
			loadSpec.Int(*f_SORT)
		}
		querySpec.OrderBy = *f_SORT
	} else {
		querySpec.OrderBy = ""
	}

	if *f_TIME {
		// TODO: infer the TimeBucket size
		querySpec.TimeBucket = 60 * 60 * 24
		loadSpec.Int(*f_TIME_COL)
	}

	querySpec.Limit = int16(*f_LIMIT)

	if *f_SESSION_COL != "" {
		loadSpec.Str(*f_SESSION_COL)
	}

	if !*f_PRINT_INFO {
		// DISABLE GC FOR QUERY PATH
		// NEVER TURN IT BACK ON!
		log.Println("ADDING BULLET HOLES FOR SPEED (DISABLING GC)")
		old_percent := debug.SetGCPercent(-1)

		log.Println("USING LOAD SPEC", loadSpec)

		log.Println("USING QUERY SPEC", querySpec)

		var count int
		start := time.Now()

		if !cached_table_info {
			log.Println("COULDN'T FIND CACHED TABLE INFO, WILL LOAD RECORDS THEN QUERY")
			f_UPDATE_TABLE_INFO = &TRUE
			f_LOAD_AND_QUERY = &FALSE
		}

		if *f_LOAD_AND_QUERY {
			count = t.LoadAndQueryRecords(&loadSpec, &querySpec)
			end := time.Now()
			log.Println("LOAD AND QUERY RECORDS TOOK", end.Sub(start))
			querySpec.printResults()
		} else {
			count = t.LoadRecords(&loadSpec)
			end := time.Now()
			log.Println("LOAD RECORDS TOOK", end.Sub(start))
		}

		if *f_LOAD_AND_QUERY == false {
			if count > MAX_RECORDS_NO_GC {
				log.Println("MORE THAN", fmt.Sprintf("%dm", MAX_RECORDS_NO_GC/1000/1000), "RECORDS LOADED ENABLING GC")
				gc_start := time.Now()
				debug.SetGCPercent(old_percent)
				end := time.Now()
				log.Println("GC TOOK", end.Sub(gc_start))
			}

			queryTable(table, &loadSpec, &querySpec)
			end := time.Now()
			log.Println("LOADING & QUERYING TABLE TOOK", end.Sub(start))
		}
	}

	if *f_PRINT_INFO {
		t := GetTable(table)
		t.PrintColInfo()
	}

	if *f_UPDATE_TABLE_INFO {
		t.SaveTableInfo("info")
	}
}
