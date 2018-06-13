package sybil

import "flag"

import "os"
import "encoding/gob"

func init() {
	setDefaults()
}

var TEST_MODE = false

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

	TIME        bool
	TIME_COL    string
	TIME_BUCKET int
	HIST_BUCKET int
	LOG_HIST    bool

	FIELD_SEPARATOR    string
	FILTER_SEPARATOR   string
	LOAD_AND_QUERY     bool
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

	// STATS
	ANOVA_ICC bool

	WRITE_BLOCK_INFO bool
}

type StrReplace struct {
	Pattern string
	Replace string
}

var FLAGS = FlagDefs{}

func setDefaults() {
	FLAGS.GC = true
	FLAGS.PRINT = true

	FLAGS.LOAD_AND_QUERY = true
	flag.StringVar(&FLAGS.DIR, "dir", "./db/", "Directory to store DB files")
	flag.StringVar(&FLAGS.TABLE, "table", "", "Table to operate on [REQUIRED]")

	flag.BoolVar(&FLAGS.DEBUG, "debug", false, "enable debug logging")
	flag.StringVar(&FLAGS.FIELD_SEPARATOR, "field-separator", ",", "Field separator used in command line params")
	flag.StringVar(&FLAGS.FILTER_SEPARATOR, "filter-separator", ":", "Filter separator used in filters")

	FLAGS.SKIP_OUTLIERS = true
	FLAGS.RECYCLE_MEM = true

	FLAGS.LIMIT = 100

	if PROFILER_ENABLED {
		flag.BoolVar(&FLAGS.PROFILE, "profile", false, "turn profiling on?")
		flag.BoolVar(&FLAGS.PROFILE_MEM, "mem", false, "turn memory profiling on")
	}

	FLAGS.WRITE_BLOCK_INFO = false

}

func EncodeFlags() {
	oldEncode := FLAGS.ENCODE_FLAGS
	FLAGS.ENCODE_FLAGS = false
	PrintBytes(FLAGS)
	FLAGS.ENCODE_FLAGS = oldEncode
}

func DecodeFlags() {
	Debug("READING ENCODED FLAGS FROM STDIN")
	dec := gob.NewDecoder(os.Stdin)
	err := dec.Decode(&FLAGS)
	if err != nil {
		Error("ERROR DECODING FLAGS", err)
	}
}
