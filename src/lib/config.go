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
	OP          string
	PRINT       bool // print results out
	EXPORT      bool // save records that match filter to tsv files
	LIST_TABLES bool // list the tables in the db dir

	// for usage with distributed queries
	DECODE_FLAGS   bool // load query flags from stdin as gob encoded data
	ENCODE_FLAGS   bool // print the query flags to stdout as binary
	ENCODE_RESULTS bool // print the querySpec results to stdout as binary

	INT_FILTERS string
	STR_FILTERS string
	STR_REPLACE string // regex replacement for strings
	SET_FILTERS string

	INTS     string
	STRS     string
	GROUPS   string
	DISTINCT string

	ADD_RECORDS int

	TIME        bool
	TIME_COL    string
	TIME_BUCKET int
	HIST_BUCKET int
	HDR_HIST    bool
	LOG_HIST    bool

	FIELD_SEPARATOR    string
	FILTER_SEPARATOR   string
	PRINT_KEYS         bool
	LOAD_AND_QUERY     bool
	LOAD_THEN_QUERY    bool
	READ_INGESTION_LOG bool
	READ_ROWSTORE      bool
	SKIP_COMPACT       bool

	PROFILE     bool
	PROFILE_MEM bool

	RECYCLE_MEM    bool
	CACHED_QUERIES bool

	WEIGHT_COL string

	LIMIT int

	DEBUG bool
	JSON  bool
	GC    bool

	DIR        string
	SORT       string
	PRUNE_BY   string
	TABLE      string
	PRINT_INFO bool
	SAMPLES    bool

	UPDATE_TABLE_INFO bool
	SKIP_OUTLIERS     bool
}

type StrReplace struct {
	Pattern string
	Replace string
}

type OptionDefs struct {
	STR_REPLACEMENTS map[string]StrReplace
	WEIGHT_COL       bool
	WEIGHT_COL_ID    int16
	WRITE_BLOCK_INFO bool
	TIME_COL_ID      int16
	TIME_FORMAT      string
	MERGE_TABLE      *Table
}

// TODO: merge these two into one thing
// current problem is that FLAGS needs pointers
var FLAGS = FlagDefs{}
var OPTS = OptionDefs{}
var EMPTY = ""

func setDefaults() {
	OPTS.WEIGHT_COL = false
	OPTS.WEIGHT_COL_ID = int16(0)
	OPTS.WRITE_BLOCK_INFO = false
	OPTS.TIME_FORMAT = "2006-01-02 15:04:05.999999999 -0700 MST"

	FLAGS.GC = true
	FLAGS.JSON = false
	FLAGS.PRINT = true
	FLAGS.EXPORT = false
	FLAGS.LIST_TABLES = false

	FLAGS.ENCODE_RESULTS = false
	FLAGS.ENCODE_FLAGS = false
	FLAGS.DECODE_FLAGS = false

	FLAGS.SKIP_COMPACT = false

	FLAGS.LOAD_AND_QUERY = true
	FLAGS.LOAD_THEN_QUERY = false
	FLAGS.READ_INGESTION_LOG = false
	FLAGS.READ_ROWSTORE = false
	flag.StringVar(&FLAGS.DIR, "dir", "./db/", "Directory to store DB files")
	flag.StringVar(&FLAGS.TABLE, "table", "", "Table to operate on [REQUIRED]")

	flag.BoolVar(&FLAGS.DEBUG, "debug", false, "enable debug logging")
	flag.StringVar(&FLAGS.FIELD_SEPARATOR, "field-separator", ",", "Field separator used in command line params")
	flag.StringVar(&FLAGS.FILTER_SEPARATOR, "filter-separator", ":", "Filter separator used in filters")

	FLAGS.UPDATE_TABLE_INFO = false
	FLAGS.SKIP_OUTLIERS = true
	FLAGS.SAMPLES = false

	FLAGS.RECYCLE_MEM = true
	FLAGS.CACHED_QUERIES = false

	FLAGS.HDR_HIST = false
	FLAGS.LOG_HIST = false

	DEFAULT_LIMIT := 100
	FLAGS.LIMIT = DEFAULT_LIMIT

	FLAGS.PROFILE = false
	FLAGS.PROFILE_MEM = false
	if PROFILER_ENABLED {
		flag.BoolVar(&FLAGS.PROFILE, "profile", false, "turn profiling on?")
		flag.BoolVar(&FLAGS.PROFILE_MEM, "mem", false, "turn memory profiling on")
	}

}

func EncodeFlags() {
	old_encode := FLAGS.ENCODE_FLAGS
	FLAGS.ENCODE_FLAGS = false
	PrintBytes(FLAGS)
	FLAGS.ENCODE_FLAGS = old_encode
}

func DecodeFlags() {
	Debug("READING ENCODED FLAGS FROM STDIN")
	dec := gob.NewDecoder(os.Stdin)
	err := dec.Decode(&FLAGS)
	if err != nil {
		Error("ERROR DECODING FLAGS", err)
	}
}
