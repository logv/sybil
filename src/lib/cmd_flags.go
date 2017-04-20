package sybil

import "flag"

var FALSE = false
var TRUE = true

var TEST_MODE = false
var ENABLE_LUA = false

type FlagDefs struct {
	OP          *string
	PRINT       *bool
	INT_FILTERS *string
	STR_FILTERS *string
	STR_REPLACE *string // regex replacement for strings
	SET_FILTERS *string

	SESSION_COL *string
	INTS        *string
	STRS        *string
	GROUPS      *string

	ADD_RECORDS *int

	TIME        *bool
	TIME_COL    *string
	TIME_BUCKET *int
	HIST_BUCKET *int

	PRINT_KEYS         *bool
	LOAD_AND_QUERY     *bool
	LOAD_THEN_QUERY    *bool
	READ_INGESTION_LOG *bool
	READ_ROWSTORE      *bool

	PROFILE     *bool
	PROFILE_MEM *bool

	RECYCLE_MEM *bool

	WEIGHT_COL *string

	LIMIT *int

	JSON *bool
	GC   *bool

	DIR        *string
	SORT       *string
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
	pattern string
	replace string
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
}

// TODO: merge these two into one thing
// current problem is that FLAGS needs pointers
var FLAGS = FlagDefs{}
var OPTS = OptionDefs{}
var EMPTY = ""

func SetDefaults() {
	OPTS.SORT_COUNT = "$COUNT"
	OPTS.SAMPLES = false
	OPTS.WEIGHT_COL = false
	OPTS.WEIGHT_COL_ID = int16(0)
	OPTS.DELTA_ENCODE_INT_VALUES = true
	OPTS.DELTA_ENCODE_RECORD_IDS = true
	OPTS.WRITE_BLOCK_INFO = false
	OPTS.TIMESERIES = false
	OPTS.TIME_FORMAT = "2006-01-02 15:04:05.999999999 -0700 MST"

	FLAGS.GC = &TRUE
	FLAGS.JSON = &FALSE
	FLAGS.PRINT = &TRUE

	FLAGS.PRINT_KEYS = &OPTS.TIMESERIES
	FLAGS.LOAD_AND_QUERY = &TRUE
	FLAGS.LOAD_THEN_QUERY = &FALSE
	FLAGS.READ_INGESTION_LOG = &FALSE
	FLAGS.READ_ROWSTORE = &FALSE
	FLAGS.ANOVA_ICC = &FALSE
	FLAGS.DIR = flag.String("dir", "./db/", "Directory to store DB files")
	FLAGS.TABLE = flag.String("table", "", "Table to operate on [REQUIRED]")

	FLAGS.UPDATE_TABLE_INFO = &FALSE
	FLAGS.SKIP_OUTLIERS = &TRUE
	FLAGS.SAMPLES = &FALSE
	FLAGS.LUA = &FALSE
	FLAGS.LUAFILE = &EMPTY

	FLAGS.RECYCLE_MEM = &TRUE

	DEFAULT_LIMIT := 100
	FLAGS.LIMIT = &DEFAULT_LIMIT

	FLAGS.PROFILE = &FALSE
	FLAGS.PROFILE_MEM = &FALSE
	if PROFILER_ENABLED {
		FLAGS.PROFILE = flag.Bool("profile", false, "turn profiling on?")
		FLAGS.PROFILE_MEM = flag.Bool("mem", false, "turn memory profiling on")
	}

	initLua()
}
