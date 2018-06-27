package cmd

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"runtime/debug"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/jsonpb"
	"github.com/logv/sybil/src/sybil"
	pb "github.com/logv/sybil/src/sybilpb"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var MAX_RECORDS_NO_GC = 4 * 1000 * 1000 // 4 million

const (
	SORT_COUNT = "$COUNT"
)

func addPrintFlags() {
	flag.StringVar(&sybil.FLAGS.OP, "op", "avg", "metric to calculate, either 'avg' or 'hist'")
	flag.BoolVar(&sybil.FLAGS.LIST_TABLES, "tables", false, "List tables")
	flag.BoolVar(&sybil.FLAGS.PRINT_INFO, "info", false, "Print table info")
	flag.IntVar(&sybil.FLAGS.LIMIT, "limit", 100, "Number of results to return")
	flag.BoolVar(&sybil.FLAGS.PRINT, "print", true, "Print some records")
	flag.BoolVar(&sybil.FLAGS.SAMPLES, "samples", false, "Grab samples")
	flag.BoolVar(&sybil.FLAGS.JSON, "json", false, "Print results in JSON format")
}

func addQueryFlags() {

	flag.StringVar(&sybil.FLAGS.SORT, "sort", SORT_COUNT, "Int Column to sort by")
	flag.StringVar(&sybil.FLAGS.PRUNE_BY, "prune-sort", SORT_COUNT, "Int Column to prune intermediate results by")

	flag.BoolVar(&sybil.FLAGS.TIME, "time", false, "make a time rollup")
	flag.StringVar(&sybil.FLAGS.TIME_COL, "time-col", "time", "which column to treat as a timestamp (use with -time flag)")
	flag.IntVar(&sybil.FLAGS.TIME_BUCKET, "time-bucket", 60*60, "time bucket (in seconds)")
	flag.StringVar(&sybil.FLAGS.WEIGHT_COL, "weight-col", "", "Which column to treat as an optional weighting column")

	flag.BoolVar(&sybil.FLAGS.LOG_HIST, "loghist", false, "Use nested logarithmic histograms")

	flag.BoolVar(&sybil.FLAGS.ENCODE_RESULTS, "encode-results", false, "Print the results in binary format")
	flag.BoolVar(&sybil.FLAGS.ENCODE_FLAGS, "encode-flags", false, "Print the query flags in binary format")
	flag.BoolVar(&sybil.FLAGS.DECODE_FLAGS, "decode-flags", false, "Use the query flags supplied on stdin")
	flag.StringVar(&sybil.FLAGS.INT_FILTERS, "int-filter", "", "Int filters, format: col:op:val")

	flag.IntVar(&sybil.FLAGS.HIST_BUCKET, "int-bucket", 0, "Int hist bucket size")

	flag.StringVar(&sybil.FLAGS.STR_REPLACE, "str-replace", "", "Str replacement, format: col:find:replace")
	flag.StringVar(&sybil.FLAGS.STR_FILTERS, "str-filter", "", "Str filters, format: col:op:val")
	flag.StringVar(&sybil.FLAGS.SET_FILTERS, "set-filter", "", "Set filters, format: col:op:val")
	flag.BoolVar(&sybil.FLAGS.UPDATE_TABLE_INFO, "update-info", false, "Re-compute cached column data")

	flag.StringVar(&sybil.FLAGS.INTS, "int", "", "Integer values to aggregate")
	flag.StringVar(&sybil.FLAGS.STRS, "str", "", "String values to load")
	flag.StringVar(&sybil.FLAGS.GROUPS, "group", "", "values group by")
	flag.StringVar(&sybil.FLAGS.DISTINCT, sybil.OP_DISTINCT, "", "distinct group by")

	flag.BoolVar(&sybil.FLAGS.EXPORT, "export", false, "export data to TSV")

	flag.BoolVar(&sybil.FLAGS.READ_ROWSTORE, "read-log", false, "read the ingestion log (can take longer!)")

	flag.BoolVar(&sybil.FLAGS.ANOVA_ICC, "icc", false, "Calculate intraclass co-efficient (ANOVA)")

	flag.BoolVar(&sybil.FLAGS.RECYCLE_MEM, "recycle-mem", true, "recycle memory slabs (versus using Go's GC)")

	flag.BoolVar(&sybil.FLAGS.CACHED_QUERIES, "cache-queries", false, "Cache query results per block")

}

func RunQueryCmdLine() {
	addQueryFlags()
	addPrintFlags()
	flag.Parse()
	if err := runQueryCmdLine(&sybil.FLAGS); err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrap(err, "query"))
		os.Exit(1)
	}
}

func runQueryCmdLine(flags *sybil.FlagDefs) error {
	if flags.DIAL != "" {
		return runQueryGRPC(flags)
	}
	if flags.DECODE_FLAGS {
		sybil.DecodeFlags()
	}

	if flags.ENCODE_FLAGS {
		sybil.Debug("PRINTING ENCODED FLAGS")
		sybil.EncodeFlags()
		return nil
	}

	printSpec := &sybil.PrintSpec{
		ListTables: flags.LIST_TABLES,
		PrintInfo:  flags.PRINT_INFO,
		Samples:    flags.SAMPLES,

		Op:            sybil.Op(flags.OP),
		Limit:         flags.LIMIT,
		EncodeResults: flags.ENCODE_RESULTS,
		JSON:          flags.JSON,
	}
	if flags.LIST_TABLES {
		sybil.PrintTables(printSpec)
		return nil
	}

	table := flags.TABLE
	if table == "" {
		flag.PrintDefaults()
		return nil
	}

	t := sybil.GetTable(table)
	if t.IsNotExist() {
		return fmt.Errorf("table %v does not exist in %v", flags.TABLE, flags.DIR)
	}

	ints := make([]string, 0)
	groups := make([]string, 0)
	strs := make([]string, 0)
	distinct := make([]string, 0)

	if flags.GROUPS != "" {
		groups = strings.Split(flags.GROUPS, flags.FIELD_SEPARATOR)
	}

	if flags.DISTINCT != "" {
		distinct = strings.Split(flags.DISTINCT, flags.FIELD_SEPARATOR)
	}

	// PROCESS CMD LINE ARGS THAT USE COMMA DELIMITERS
	if flags.STRS != "" {
		strs = strings.Split(flags.STRS, flags.FIELD_SEPARATOR)
	}
	if flags.INTS != "" {
		ints = strings.Split(flags.INTS, flags.FIELD_SEPARATOR)
	}
	if flags.PROFILE && sybil.PROFILER_ENABLED {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	if flags.READ_ROWSTORE {
		flags.READ_INGESTION_LOG = true
	}

	// LOAD TABLE INFOS BEFORE WE CREATE OUR FILTERS, SO WE CAN CREATE FILTERS ON
	// THE RIGHT COLUMN ID
	if err := t.LoadTableInfo(); err != nil {
		return errors.Wrap(err, "load table info failed")
	}
	if _, err := t.LoadRecords(nil); err != nil {
		return errors.Wrap(err, "loading records failed")
	}

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
	var op sybil.Op
	switch sybil.Op(flags.OP) {
	case sybil.OP_HIST:
		op = sybil.OP_HIST
	case sybil.OP_AVG:
		op = sybil.OP_AVG
	case sybil.OP_DISTINCT:
		op = sybil.OP_DISTINCT
	}
	for _, agg := range ints {
		aggs = append(aggs, t.Aggregation(agg, op))
	}

	distincts := []sybil.Grouping{}
	for _, g := range distinct {
		distincts = append(distincts, t.Grouping(g))
	}

	if op == sybil.OP_DISTINCT {
		distincts = groupings
		groupings = make([]sybil.Grouping, 0)
	}

	// VERIFY THE KEY TABLE IS IN ORDER, OTHERWISE WE NEED TO EXIT
	sybil.Debug("KEY TABLE", t.KeyTable)
	sybil.Debug("KEY TYPES", t.KeyTypes)

	used := make(map[int16]int)
	for _, v := range t.KeyTable {
		used[v]++
		if used[v] > 1 {
			return sybil.ErrKeyTableInconsistent
		}
	}

	loadSpec := t.NewLoadSpec()
	filterSpec := sybil.FilterSpec{Int: flags.INT_FILTERS, Str: flags.STR_FILTERS, Set: flags.SET_FILTERS}
	filters, err := sybil.BuildFilters(t, &loadSpec, filterSpec)
	if err != nil {
		return err
	}

	replacements := sybil.BuildReplacements(flags.FIELD_SEPARATOR, flags.STR_REPLACE)
	queryParams := sybil.QueryParams{
		Groups:       groupings,
		Filters:      filters,
		Aggregations: aggs,
		Distincts:    distincts,

		CachedQueries: flags.CACHED_QUERIES,
		StrReplace:    replacements,
	}
	if op == sybil.OP_HIST {
		histType := sybil.HistogramTypeBasic
		if flags.LOG_HIST {
			histType = sybil.HistogramTypeLog
		}
		queryParams.HistogramParameters = sybil.HistogramParameters{
			Type:       histType,
			BucketSize: flags.HIST_BUCKET,
			Weighted:   flags.WEIGHT_COL != "",
		}
	}

	querySpec := sybil.QuerySpec{QueryParams: queryParams}

	allGroups := append(groups, distinct...)
	for _, v := range allGroups {
		var err error
		switch t.GetColumnType(v) {
		case sybil.STR_VAL:
			err = loadSpec.Str(v)
		case sybil.INT_VAL:
			err = loadSpec.Int(v)
		default:
			t.PrintColInfo(printSpec)
			fmt.Println("")
			err = fmt.Errorf("Unknown column type for column: %v %v", v, t.GetColumnType(v))
		}
		if err != nil {
			return err
		}
	}
	for _, v := range strs {
		if err := loadSpec.Str(v); err != nil {
			return err
		}
	}
	for _, v := range ints {
		if err := loadSpec.Int(v); err != nil {
			return err
		}
	}

	if flags.SORT != "" {
		if flags.SORT != sybil.SORT_COUNT {
			if err := loadSpec.Int(flags.SORT); err != nil {
				return err
			}
		}
		querySpec.OrderBy = flags.SORT
	} else {
		querySpec.OrderBy = ""
	}

	if flags.PRUNE_BY != "" {
		if flags.PRUNE_BY != sybil.SORT_COUNT {
			if err := loadSpec.Int(flags.PRUNE_BY); err != nil {
				return err
			}
		}
		querySpec.PruneBy = flags.PRUNE_BY
	} else {
		querySpec.PruneBy = sybil.SORT_COUNT
	}

	if flags.TIME {
		// TODO: infer the TimeBucket size
		querySpec.TimeBucket = flags.TIME_BUCKET
		sybil.Debug("USING TIME BUCKET", querySpec.TimeBucket, "SECONDS")
		if err := loadSpec.Int(flags.TIME_COL); err != nil {
			return err
		}
	}

	if flags.WEIGHT_COL != "" {
		if err := loadSpec.Int(flags.WEIGHT_COL); err != nil {
			return err
		}
	}

	querySpec.Limit = flags.LIMIT

	if flags.SAMPLES {
		sybil.HOLD_MATCHES = true

		loadSpec := t.NewLoadSpec()
		loadSpec.LoadAllColumns = true
		loadSpec.SkipDeleteBlocksAfterQuery = true
		querySpec.Samples = true

		if _, err := t.LoadAndQueryRecords(&loadSpec, &querySpec); err != nil {
			return err
		}

		t.PrintSamples(printSpec)
		return nil
	}

	if flags.EXPORT {
		loadSpec.LoadAllColumns = true
	}

	if !flags.PRINT_INFO {
		// DISABLE GC FOR QUERY PATH
		sybil.Debug("ADDING BULLET HOLES FOR SPEED (DISABLING GC)")
		debug.SetGCPercent(-1)

		sybil.Debug("USING LOAD SPEC", loadSpec)

		sybil.Debug("USING QUERY SPEC", querySpec)

		start := time.Now()

		if flags.LOAD_AND_QUERY {
			if _, err := t.LoadAndQueryRecords(&loadSpec, &querySpec); err != nil {
				return err
			}

			end := time.Now()
			sybil.Debug("LOAD AND QUERY RECORDS TOOK", end.Sub(start))
			querySpec.PrintResults(printSpec)
		}

	}

	if flags.EXPORT {
		sybil.Print("EXPORTED RECORDS TO", path.Join(t.Name, "export"))
	}

	if flags.PRINT_INFO {
		t := sybil.GetTable(table)
		flags.LOAD_AND_QUERY = false

		if _, err := t.LoadRecords(nil); err != nil {
			return err
		}
		t.PrintColInfo(printSpec)
	}

	return nil
}

func split(s, sep string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, sep)
}

func runQueryGRPC(flags *sybil.FlagDefs) error {
	ctx := context.Background()
	opts := []grpc.DialOption{
		// todo
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(flags.DIAL, opts...)
	if err != nil {
		return err
	}
	defer conn.Close()
	c := pb.NewSybilClient(conn)

	if flags.LIST_TABLES {
		r, err := c.ListTables(ctx, &pb.ListTablesRequest{})
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(r)
	}

	if flags.PRINT_INFO {
		r, err := c.GetTable(ctx, &pb.GetTableRequest{
			Name: flags.TABLE,
		})
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(r)
	}
	q := &pb.QueryRequest{
		Dataset:         flags.TABLE,
		Ints:            split(flags.INTS, flags.FIELD_SEPARATOR),
		Strs:            split(flags.STRS, flags.FIELD_SEPARATOR),
		GroupBy:         split(flags.GROUPS, flags.FIELD_SEPARATOR),
		DistinctGroupBy: split(flags.DISTINCT, flags.FIELD_SEPARATOR),
		Limit:           int64(flags.LIMIT),
		SortBy:          flags.SORT,
		// TODO: filters
		// TODO: replacements
	}
	if flags.SAMPLES {
		q.Type = pb.QueryType_SAMPLES
	}
	if flags.OP == sybil.OP_AVG {
		q.Op = pb.QueryOp_AVERAGE
	} else if flags.OP == sybil.OP_HIST {
		q.Op = pb.QueryOp_HISTOGRAM
		q.Type = pb.QueryType_DISTRIBUTION
	}
	qr, err := c.Query(ctx, q)
	if err != nil {
		return err
	}
	return (&jsonpb.Marshaler{}).Marshal(os.Stdout, qr)
}
