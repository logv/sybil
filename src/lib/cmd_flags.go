package sybil

import "flag"

var FALSE = false
var TRUE = true

var TEST_MODE = false

type FlagDefs struct {
	OP          *string
	PRINT       *bool
	INT_FILTERS *string
	STR_FILTERS *string
	SET_FILTERS *string

	SESSION_COL *string
	INTS        *string
	STRS        *string
	GROUPS      *string

	ADD_RECORDS *int

	TIME        *bool
	TIME_COL    *string
	TIME_BUCKET *int

	PRINT_KEYS         *bool
	LOAD_AND_QUERY     *bool
	LOAD_THEN_QUERY    *bool
	READ_INGESTION_LOG *bool
	SKIP_ROWSTORE      *bool

	PROFILE     *bool
	PROFILE_MEM *bool

	WEIGHT_COL *string

	LIMIT *int

	JSON *bool
	GC   *bool

	DIR        *string
	SORT       *string
	TABLE      *string
	PRINT_INFO *bool
	SAMPLES    *bool

	UPDATE_TABLE_INFO *bool
}

type OptionDefs struct {
	SORT_COUNT              string
	SAMPLES                 bool
	WEIGHT_COL              bool
	WEIGHT_COL_ID           int16
	DELTA_ENCODE_INT_VALUES bool
	DELTA_ENCODE_RECORD_IDS bool
	WRITE_BLOCK_INFO        bool
	TIMESERIES              bool
	TIME_COL_ID             int16
	GROUP_BY                []string
}

var FLAGS = FlagDefs{}
var OPTS = OptionDefs{}

func SetDefaults() {
	OPTS.SORT_COUNT = "$COUNT"
	OPTS.SAMPLES = false
	OPTS.WEIGHT_COL = false
	OPTS.WEIGHT_COL_ID = int16(0)
	OPTS.DELTA_ENCODE_INT_VALUES = true
	OPTS.DELTA_ENCODE_RECORD_IDS = true
	OPTS.WRITE_BLOCK_INFO = false
	OPTS.TIMESERIES = false

	FLAGS.GC = &TRUE
	FLAGS.JSON = &FALSE

	FLAGS.PRINT_KEYS = &OPTS.TIMESERIES
	FLAGS.LOAD_AND_QUERY = &TRUE
	FLAGS.LOAD_THEN_QUERY = &FALSE
	FLAGS.READ_INGESTION_LOG = &TRUE
	FLAGS.SKIP_ROWSTORE = &FALSE
	FLAGS.DIR = flag.String("dir", "./db/", "Directory to store DB files")
	FLAGS.TABLE = flag.String("table", "", "Table to operate on [REQUIRED]")
	FLAGS.PRINT_INFO = flag.Bool("info", false, "Print table info")
	FLAGS.SORT = flag.String("sort", OPTS.SORT_COUNT, "Int Column to sort by")
	FLAGS.LIMIT = flag.Int("limit", 100, "Number of results to return")

	FLAGS.UPDATE_TABLE_INFO = &FALSE
	FLAGS.SAMPLES = &FALSE

	FLAGS.PROFILE = &FALSE
	FLAGS.PROFILE_MEM = &FALSE
	if PROFILER_ENABLED {
		FLAGS.PROFILE = flag.Bool("profile", false, "turn profiling on?")
		FLAGS.PROFILE_MEM = flag.Bool("mem", false, "turn memory profiling on")
	}

}
