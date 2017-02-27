package sybil_cmd

import sybil "github.com/logv/sybil/src/lib"

import "flag"
import "time"
import "runtime/debug"
import "strings"

func addSessionFlags() {
	sybil.FLAGS.PRINT = flag.Bool("print", false, "Print some records")
	sybil.FLAGS.TIME_COL = flag.String("time-col", "time", "which column to treat as a timestamp (use with -time flag)")
	sybil.FLAGS.SESSION_COL = flag.String("session", "", "Column to use for sessionizing")
	sybil.FLAGS.SESSION_CUTOFF = flag.Int("cutoff", 60, "distance between consecutive events before generating a new session")
	sybil.FLAGS.JOIN_TABLE = flag.String("join-table", "", "dataset to join against for session summaries")
	sybil.FLAGS.JOIN_KEY = flag.String("join-key", "", "Field to join sessionid against in join-table")
	sybil.FLAGS.JOIN_GROUP = flag.String("join-group", "", "Group by columns to pull from join record")
	sybil.FLAGS.PATH_KEY = flag.String("path-key", "", "Field to use for pathing")
	sybil.FLAGS.PATH_LENGTH = flag.Int("path-length", 3, "Size of paths to histogram")
	sybil.FLAGS.RETENTION = flag.Bool("calendar", false, "calculate retention calendars")
	sybil.FLAGS.JSON = flag.Bool("json", false, "print results in JSON form")

	sybil.FLAGS.INT_FILTERS = flag.String("int-filter", "", "Int filters, format: col:op:val")
	sybil.FLAGS.STR_FILTERS = flag.String("str-filter", "", "Str filters, format: col:op:val")
	sybil.FLAGS.SET_FILTERS = flag.String("set-filter", "", "Set filters, format: col:op:val")

	sybil.FLAGS.STR_REPLACE = flag.String("str-replace", "", "Str replacement, format: col:find:replace")
	sybil.FLAGS.LIMIT = flag.Int("limit", 100, "Number of results to return")
}

func RunSessionizeCmdLine() {
	addSessionFlags()
	flag.Parse()
	start := time.Now()

	table := *sybil.FLAGS.TABLE
	if table == "" {
		flag.PrintDefaults()
		return
	}

	table_names := strings.Split(table, ",")
	sybil.Debug("LOADING TABLES", table_names)

	tables := make([]*sybil.Table, 0)

	for _, tablename := range table_names {
		t := sybil.GetTable(tablename)
		// LOAD TABLE INFOS BEFORE WE CREATE OUR FILTERS, SO WE CAN CREATE FILTERS ON
		// THE RIGHT COLUMN ID
		t.LoadTableInfo()
		t.LoadRecords(nil)

		count := 0
		for _, block := range t.BlockList {
			count += int(block.Info.NumRecords)
		}

		sybil.Debug("WILL INSPECT", count, "RECORDS FROM", tablename)

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

		tables = append(tables, t)

	}

	debug.SetGCPercent(-1)
	if *sybil.FLAGS.PROFILE && sybil.PROFILER_ENABLED {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	filters := []sybil.Filter{}
	groupings := []sybil.Grouping{}
	aggs := []sybil.Aggregation{}
	querySpec := sybil.QuerySpec{Groups: groupings, Filters: filters, Aggregations: aggs}

	querySpec.Limit = int16(*sybil.FLAGS.LIMIT)

	if *sybil.FLAGS.SESSION_COL != "" {
		sessionSpec := sybil.NewSessionSpec()
		sybil.LoadAndSessionize(tables, &querySpec, &sessionSpec)
	}

	end := time.Now()
	sybil.Debug("LOAD AND QUERY RECORDS TOOK", end.Sub(start))
}
