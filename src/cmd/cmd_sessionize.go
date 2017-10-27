package cmd

import (
	"flag"
	"runtime/debug"
	"strings"
	"time"

	. "github.com/logv/sybil/src/exp/query_sessions"
	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/query/load_and_query"
	. "github.com/logv/sybil/src/query/specs"
	. "github.com/logv/sybil/src/storage/metadata_io"
)

func addSessionFlags() {
	FLAGS.PRINT = flag.Bool("print", false, "Print some records")
	FLAGS.TIME_COL = flag.String("time-col", "time", "which column to treat as a timestamp (use with -time flag)")
	FLAGS.SESSION_COL = flag.String("session", "", "Column to use for sessionizing")
	FLAGS.SESSION_CUTOFF = flag.Int("cutoff", 60, "distance between consecutive events before generating a new session")
	FLAGS.JOIN_TABLE = flag.String("join-table", "", "dataset to join against for session summaries")
	FLAGS.JOIN_KEY = flag.String("join-key", "", "Field to join sessionid against in join-table")
	FLAGS.JOIN_GROUP = flag.String("join-group", "", "Group by columns to pull from join record")
	FLAGS.PATH_KEY = flag.String("path-key", "", "Field to use for pathing")
	FLAGS.PATH_LENGTH = flag.Int("path-length", 3, "Size of paths to histogram")
	FLAGS.RETENTION = flag.Bool("calendar", false, "calculate retention calendars")
	FLAGS.JSON = flag.Bool("json", false, "print results in JSON form")

	FLAGS.INT_FILTERS = flag.String("int-filter", "", "Int filters, format: col:op:val")
	FLAGS.STR_FILTERS = flag.String("str-filter", "", "Str filters, format: col:op:val")
	FLAGS.SET_FILTERS = flag.String("set-filter", "", "Set filters, format: col:op:val")

	FLAGS.STR_REPLACE = flag.String("str-replace", "", "Str replacement, format: col:find:replace")
	FLAGS.LIMIT = flag.Int("limit", 100, "Number of results to return")
}

func RunSessionizeCmdLine() {
	addSessionFlags()
	flag.Parse()
	start := time.Now()

	table := *FLAGS.TABLE
	if table == "" {
		flag.PrintDefaults()
		return
	}

	table_names := strings.Split(table, *FLAGS.FIELD_SEPARATOR)
	Debug("LOADING TABLES", table_names)

	tables := make([]*Table, 0)

	for _, tablename := range table_names {
		t := GetTable(tablename)
		// LOAD TABLE INFOS BEFORE WE CREATE OUR FILTERS, SO WE CAN CREATE FILTERS ON
		// THE RIGHT COLUMN ID
		LoadTableInfo(t)
		LoadRecords(t, nil)

		count := 0
		for _, block := range t.BlockList {
			count += int(block.Info.NumRecords)
		}

		Debug("WILL INSPECT", count, "RECORDS FROM", tablename)

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

		tables = append(tables, t)

	}

	debug.SetGCPercent(-1)
	if *FLAGS.PROFILE && PROFILER_ENABLED {
		profile := RUN_PROFILER()
		defer profile.Start().Stop()
	}

	filters := []Filter{}
	groupings := []Grouping{}
	aggs := []Aggregation{}
	query_params := QueryParams{Groups: groupings, Filters: filters, Aggregations: aggs}
	querySpec := QuerySpec{QueryParams: query_params}

	querySpec.Limit = int16(*FLAGS.LIMIT)

	if *FLAGS.SESSION_COL != "" {
		sessionSpec := NewSessionSpec()
		LoadAndSessionize(tables, &querySpec, &sessionSpec)
	}

	end := time.Now()
	Debug("LOAD AND QUERY RECORDS TOOK", end.Sub(start))
}
