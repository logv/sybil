package sybil

import (
	"encoding/gob"
	"flag"
	"os"
	"sync"
)

const FALSE = false
const TRUE = true

func NewFalseFlag() *bool {
	r := false
	return &r

}

func NewTrueFlag() *bool {
	r := true
	return &r
}

var DEBUG = flag.Bool("debug", false, "enable debug logging")
var TEST_MODE = false
var ENABLE_LUA = false

type FlagDefs struct {
	OP          *string
	PRINT       *bool // print results out
	EXPORT      *bool // save records that match filter to tsv files
	LIST_TABLES *bool // list the tables in the db dir

	// for usage with distributed queries
	DECODE_FLAGS   *bool // load query flags from stdin as gob encoded data
	ENCODE_FLAGS   *bool // print the query flags to stdout as binary
	ENCODE_RESULTS *bool // print the querySpec results to stdout as binary

	INT_FILTERS *string
	STR_FILTERS *string
	STR_REPLACE *string // regex replacement for strings
	SET_FILTERS *string

	SESSION_COL *string
	INTS        *string
	STRS        *string
	GROUPS      *string
	DISTINCT    *string

	ADD_RECORDS *int

	TIME        *bool
	TIME_COL    *string
	TIME_BUCKET *int
	HIST_BUCKET *int
	HDR_HIST    *bool
	LOG_HIST    *bool

	FIELD_SEPARATOR    *string
	FILTER_SEPARATOR   *string
	PRINT_KEYS         *bool
	LOAD_THEN_QUERY    *bool
	READ_INGESTION_LOG *bool
	READ_ROWSTORE      *bool
	SKIP_COMPACT       *bool

	PROFILE     *bool
	PROFILE_MEM *bool

	RECYCLE_MEM    *bool
	CACHED_QUERIES *bool

	WEIGHT_COL *string

	LIMIT *int

	JSON *bool
	GC   *bool

	DIR        *string
	SORT       *string
	PRUNE_BY   *string
	TABLE      *string
	PRINT_INFO *bool
	SAMPLES    *bool

	LUA     *bool
	LUAFILE *string

	UPDATE_TABLE_INFO *bool
	SKIP_OUTLIERS     *bool

	// Join keys
	JOIN_TABLE *string
	JOIN_KEY   *string
	JOIN_GROUP *string

	// Sessionization stuff
	SESSION_CUTOFF *int
	RETENTION      *bool
	PATH_KEY       *string
	PATH_LENGTH    *int

	// STATS
	ANOVA_ICC *bool
}

type StrReplace struct {
	Pattern string
	Replace string
}

type OptionDefs struct {
	SORT_COUNT              string
	SAMPLES                 bool
	STR_REPLACEMENTS        map[string]StrReplace
	WEIGHT_COL              bool
	WEIGHT_COL_ID           int16
	DELTA_ENCODE_INT_VALUES bool
	DELTA_ENCODE_RECORD_IDS bool
	WRITE_BLOCK_INFO        bool
	TIMESERIES              bool
	TIME_COL_ID             int16
	TIME_FORMAT             string
	GROUP_BY                []string
	DISTINCT                []string
	MERGE_TABLE             *Table
	UPDATE_TABLE_INFO       bool
	SKIP_OUTLIERS           bool
}

var EMPTY = ""

var OPTS = defaultOptions()
var df *FlagDefs
var dfMu sync.Mutex

func DefaultFlags() *FlagDefs {
	dfMu.Lock()
	defer dfMu.Unlock()
	if df != nil {
		return df
	}
	DEFAULT_LIMIT := 100
	df = &FlagDefs{
		GC:          NewTrueFlag(),
		JSON:        NewFalseFlag(),
		PRINT:       NewTrueFlag(),
		EXPORT:      NewFalseFlag(),
		LIST_TABLES: NewFalseFlag(),

		ENCODE_RESULTS: NewFalseFlag(),
		ENCODE_FLAGS:   NewFalseFlag(),
		DECODE_FLAGS:   NewFalseFlag(),

		SKIP_COMPACT: NewFalseFlag(),

		PRINT_KEYS:         &OPTS.TIMESERIES,
		LOAD_THEN_QUERY:    NewFalseFlag(),
		READ_INGESTION_LOG: NewFalseFlag(),
		READ_ROWSTORE:      NewFalseFlag(),
		ANOVA_ICC:          NewFalseFlag(),
		DIR:                flag.String("dir", "./db/", "Directory to store DB files"),
		TABLE:              flag.String("table", "", "Table to operate on [REQUIRED]"),

		FIELD_SEPARATOR:  flag.String("field-separator", ",", "Field separator used in command line params"),
		FILTER_SEPARATOR: flag.String("filter-separator", ":", "Filter separator used in filters"),

		UPDATE_TABLE_INFO: NewFalseFlag(),
		SKIP_OUTLIERS:     NewTrueFlag(),
		SAMPLES:           NewFalseFlag(),
		LUA:               NewFalseFlag(),
		LUAFILE:           &EMPTY,

		RECYCLE_MEM:    NewTrueFlag(),
		CACHED_QUERIES: NewFalseFlag(),

		HDR_HIST: NewFalseFlag(),
		LOG_HIST: NewFalseFlag(),

		LIMIT: &DEFAULT_LIMIT,

		PROFILE:     NewFalseFlag(),
		PROFILE_MEM: NewFalseFlag(),
	}
	if PROFILER_ENABLED {
		df.PROFILE = flag.Bool("profile", false, "turn profiling on?")
		df.PROFILE_MEM = flag.Bool("mem", false, "turn memory profiling on")
	}
	return df
}

func defaultOptions() *OptionDefs {
	return &OptionDefs{
		SORT_COUNT:              "$COUNT",
		SAMPLES:                 false,
		WEIGHT_COL:              false,
		WEIGHT_COL_ID:           int16(0),
		DELTA_ENCODE_INT_VALUES: true,
		DELTA_ENCODE_RECORD_IDS: true,
		WRITE_BLOCK_INFO:        false,
		TIMESERIES:              false,
		TIME_FORMAT:             "2006-01-02 15:04:05.999999999 -0700 MST",
	}
}

func EncodeFlags(flags *FlagDefs) {
	oldEncode := flags.ENCODE_FLAGS
	flags.ENCODE_FLAGS = NewFalseFlag()
	PrintBytes(flags)
	flags.ENCODE_FLAGS = oldEncode
}

func DecodeFlags(flags *FlagDefs) {
	Debug("READING ENCODED FLAGS FROM STDIN")
	dec := gob.NewDecoder(os.Stdin)
	err := dec.Decode(flags)
	if err != nil {
		Error("ERROR DECODING FLAGS", err)
	}
}
