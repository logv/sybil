package cmd

import (
	"flag"
	"runtime/debug"
	"strings"
	"time"

	sybil "github.com/logv/sybil/src/lib"
	"github.com/logv/sybil/src/lib/common"
	"github.com/logv/sybil/src/lib/config"
)

func addSessionFlags() {
	config.FLAGS.PRINT = flag.Bool("print", false, "Print some records")
	config.FLAGS.TIME_COL = flag.String("time-col", "time", "which column to treat as a timestamp (use with -time flag)")
	config.FLAGS.SESSION_COL = flag.String("session", "", "Column to use for sessionizing")
	config.FLAGS.SESSION_CUTOFF = flag.Int("cutoff", 60, "distance between consecutive events before generating a new session")
	config.FLAGS.JOIN_TABLE = flag.String("join-table", "", "dataset to join against for session summaries")
	config.FLAGS.JOIN_KEY = flag.String("join-key", "", "Field to join sessionid against in join-table")
	config.FLAGS.JOIN_GROUP = flag.String("join-group", "", "Group by columns to pull from join record")
	config.FLAGS.PATH_KEY = flag.String("path-key", "", "Field to use for pathing")
	config.FLAGS.PATH_LENGTH = flag.Int("path-length", 3, "Size of paths to histogram")
	config.FLAGS.RETENTION = flag.Bool("calendar", false, "calculate retention calendars")
	config.FLAGS.JSON = flag.Bool("json", false, "print results in JSON form")

	config.FLAGS.INT_FILTERS = flag.String("int-filter", "", "Int filters, format: col:op:val")
	config.FLAGS.STR_FILTERS = flag.String("str-filter", "", "Str filters, format: col:op:val")
	config.FLAGS.SET_FILTERS = flag.String("set-filter", "", "Set filters, format: col:op:val")

	config.FLAGS.STR_REPLACE = flag.String("str-replace", "", "Str replacement, format: col:find:replace")
	config.FLAGS.LIMIT = flag.Int("limit", 100, "Number of results to return")
}

func RunSessionizeCmdLine() {
	addSessionFlags()
	flag.Parse()
	start := time.Now()

	table := *config.FLAGS.TABLE
	if table == "" {
		flag.PrintDefaults()
		return
	}

	table_names := strings.Split(table, *config.FLAGS.FIELD_SEPARATOR)
	common.Debug("LOADING TABLES", table_names)

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

		common.Debug("WILL INSPECT", count, "RECORDS FROM", tablename)

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

		tables = append(tables, t)

	}

	debug.SetGCPercent(-1)
	if *config.FLAGS.PROFILE && config.PROFILER_ENABLED {
		profile := config.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	filters := []sybil.Filter{}
	groupings := []sybil.Grouping{}
	aggs := []sybil.Aggregation{}
	query_params := sybil.QueryParams{Groups: groupings, Filters: filters, Aggregations: aggs}
	querySpec := sybil.QuerySpec{QueryParams: query_params}

	querySpec.Limit = int16(*config.FLAGS.LIMIT)

	if *config.FLAGS.SESSION_COL != "" {
		sessionSpec := sybil.NewSessionSpec()
		sybil.LoadAndSessionize(tables, &querySpec, &sessionSpec)
	}

	end := time.Now()
	common.Debug("LOAD AND QUERY RECORDS TOOK", end.Sub(start))
}
