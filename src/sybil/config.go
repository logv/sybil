package sybil

import "flag"

import "os"
import "encoding/gob"

func init() {
	setDefaults()
}

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
	LOG_HIST    *bool

	FIELD_SEPARATOR    *string
	FILTER_SEPARATOR   *string
	LOAD_AND_QUERY     *bool
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

	DEBUG *bool
	JSON  *bool
	GC    *bool

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
	STR_REPLACEMENTS        map[string]StrReplace
	WEIGHT_COL              bool
	WEIGHT_COL_ID           int16
	DELTA_ENCODE_INT_VALUES bool
	DELTA_ENCODE_RECORD_IDS bool
	WRITE_BLOCK_INFO        bool
	TIME_COL_ID             int16
	TIME_FORMAT             string
	GROUP_BY                []string
	DISTINCT                []string
}

// TODO: merge these two into one thing
// current problem is that FLAGS needs pointers
var FLAGS = FlagDefs{}
var OPTS = OptionDefs{}
var EMPTY = ""

func setDefaults() {
	OPTS.WEIGHT_COL = false
	OPTS.WEIGHT_COL_ID = int16(0)
	OPTS.DELTA_ENCODE_INT_VALUES = true
	OPTS.DELTA_ENCODE_RECORD_IDS = true
	OPTS.WRITE_BLOCK_INFO = false
	OPTS.TIME_FORMAT = "2006-01-02 15:04:05.999999999 -0700 MST"

	FLAGS.GC = NewTrueFlag()
	FLAGS.JSON = NewFalseFlag()
	FLAGS.PRINT = NewTrueFlag()
	FLAGS.EXPORT = NewFalseFlag()
	FLAGS.LIST_TABLES = NewFalseFlag()

	FLAGS.ENCODE_RESULTS = NewFalseFlag()
	FLAGS.ENCODE_FLAGS = NewFalseFlag()
	FLAGS.DECODE_FLAGS = NewFalseFlag()

	FLAGS.SKIP_COMPACT = NewFalseFlag()

	FLAGS.LOAD_AND_QUERY = NewTrueFlag()
	FLAGS.LOAD_THEN_QUERY = NewFalseFlag()
	FLAGS.READ_INGESTION_LOG = NewFalseFlag()
	FLAGS.READ_ROWSTORE = NewFalseFlag()
	FLAGS.ANOVA_ICC = NewFalseFlag()
	FLAGS.DIR = flag.String("dir", "./db/", "Directory to store DB files")
	FLAGS.TABLE = flag.String("table", "", "Table to operate on [REQUIRED]")

	FLAGS.DEBUG = flag.Bool("debug", false, "enable debug logging")
	FLAGS.FIELD_SEPARATOR = flag.String("field-separator", ",", "Field separator used in command line params")
	FLAGS.FILTER_SEPARATOR = flag.String("filter-separator", ":", "Filter separator used in filters")

	FLAGS.UPDATE_TABLE_INFO = NewFalseFlag()
	FLAGS.SKIP_OUTLIERS = NewTrueFlag()
	FLAGS.SAMPLES = NewFalseFlag()
	FLAGS.LUA = NewFalseFlag()
	FLAGS.LUAFILE = &EMPTY

	FLAGS.RECYCLE_MEM = NewTrueFlag()
	FLAGS.CACHED_QUERIES = NewFalseFlag()

	FLAGS.LOG_HIST = NewFalseFlag()

	DEFAULT_LIMIT := 100
	FLAGS.LIMIT = &DEFAULT_LIMIT

	FLAGS.PROFILE = NewFalseFlag()
	FLAGS.PROFILE_MEM = NewFalseFlag()
	if PROFILER_ENABLED {
		FLAGS.PROFILE = flag.Bool("profile", false, "turn profiling on?")
		FLAGS.PROFILE_MEM = flag.Bool("mem", false, "turn memory profiling on")
	}

}

func EncodeFlags() {
	oldEncode := *FLAGS.ENCODE_FLAGS
	FLAGS.ENCODE_FLAGS = NewFalseFlag()
	PrintBytes(FLAGS)
	FLAGS.ENCODE_FLAGS = &oldEncode
}

func DecodeFlags() {
	Debug("READING ENCODED FLAGS FROM STDIN")
	dec := gob.NewDecoder(os.Stdin)
	err := dec.Decode(&FLAGS)
	if err != nil {
		Error("ERROR DECODING FLAGS", err)
	}
}
