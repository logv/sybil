package sybil

import "flag"

var falseFlag = false
var trueFlag = true

var TestMode = false
var EnableLua = false

type FlagDefs struct {
	Op         *string
	Print      *bool
	Export     *bool
	IntFilters *string
	StrFilters *string
	StrReplace *string // regex replacement for strings
	SetFilters *string

	SessionCol *string
	Ints       *string
	Strs       *string
	Groups     *string

	AddRecords *int

	Time       *bool
	TimeCol    *string
	TimeBucket *int
	HistBucket *int
	HdrHist    *bool
	LogHist    *bool

	FieldSeparator   *string
	FilterSeparator  *string
	PrintKeys        *bool
	LoadAndQuery     *bool
	LoadThenQuery    *bool
	ReadIngestionLog *bool
	ReadRowstore     *bool
	SkipCompact      *bool

	Profile    *bool
	ProfileMem *bool

	RecycleMem    *bool
	CachedQueries *bool

	WeightCol *string

	Limit *int

	Debug *bool
	JSON  *bool
	GC    *bool

	Dir       *string
	Sort      *string
	Table     *string
	PrintInfo *bool
	Samples   *bool

	LUA     *bool
	Luafile *string

	UpdateTableInfo *bool
	SkipOutliers    *bool

	// Join keys
	JoinTable *string
	JoinKey   *string
	JoinGroup *string

	// Sessionization stuff
	SessionCutoff *int
	Retention     *bool
	PathKey       *string
	PathLength    *int

	// STATS
	AnovaIcc *bool
}

type StrReplace struct {
	pattern string
	replace string
}

type OptionDefs struct {
	SortCount            string
	Samples              bool
	StrReplacements      map[string]StrReplace
	WeightCol            bool
	WeightColID          int16
	DeltaEncodeIntValues bool
	DeltaEncodeRecordIDs bool
	WriteBlockInfo       bool
	TimeSeries           bool
	TimeColID            int16
	TimeFormat           string
	GroupBy              []string
}

// TODO: merge these two into one thing
// current problem is that FLAGS needs pointers
var FLAGS = FlagDefs{}
var OPTS = OptionDefs{}
var EMPTY = ""

func setDefaults() {
	OPTS.SortCount = "$COUNT"
	OPTS.Samples = false
	OPTS.WeightCol = false
	OPTS.WeightColID = int16(0)
	OPTS.DeltaEncodeIntValues = true
	OPTS.DeltaEncodeRecordIDs = true
	OPTS.WriteBlockInfo = false
	OPTS.TimeSeries = false
	OPTS.TimeFormat = "2006-01-02 15:04:05.999999999 -0700 MST"

	FLAGS.GC = &trueFlag
	FLAGS.JSON = &falseFlag
	FLAGS.Print = &trueFlag
	FLAGS.Export = &falseFlag

	FLAGS.SkipCompact = &falseFlag

	FLAGS.PrintKeys = &OPTS.TimeSeries
	FLAGS.LoadAndQuery = &trueFlag
	FLAGS.LoadThenQuery = &falseFlag
	FLAGS.ReadIngestionLog = &falseFlag
	FLAGS.ReadRowstore = &falseFlag
	FLAGS.AnovaIcc = &falseFlag
	FLAGS.Dir = flag.String("dir", "./db/", "Directory to store DB files")
	FLAGS.Table = flag.String("table", "", "Table to operate on [REQUIRED]")

	FLAGS.Debug = flag.Bool("debug", false, "enable debug logging")
	FLAGS.FieldSeparator = flag.String("field-separator", ",", "Field separator used in command line params")
	FLAGS.FilterSeparator = flag.String("filter-separator", ":", "Filter separator used in filters")

	FLAGS.UpdateTableInfo = &falseFlag
	FLAGS.SkipOutliers = &trueFlag
	FLAGS.Samples = &falseFlag
	FLAGS.LUA = &falseFlag
	FLAGS.Luafile = &EMPTY

	FLAGS.RecycleMem = &trueFlag
	FLAGS.CachedQueries = &falseFlag

	FLAGS.HdrHist = &falseFlag
	FLAGS.LogHist = &falseFlag

	DefaultLimit := 100
	FLAGS.Limit = &DefaultLimit

	FLAGS.Profile = &falseFlag
	FLAGS.ProfileMem = &falseFlag
	if ProfilerEnabled {
		FLAGS.Profile = flag.Bool("profile", false, "turn profiling on?")
		FLAGS.ProfileMem = flag.Bool("mem", false, "turn memory profiling on")
	}

	initLua()

}
