package pcs

import "flag"

var FALSE = false
var TRUE = true

var f_OP *string
var f_PRINT *bool = &FALSE
var f_INT_FILTERS *string
var f_STR_FILTERS *string

var f_SESSION_COL *string
var f_INTS *string
var f_STRS *string
var f_GROUPS *string

var GROUP_BY []string

var f_ADD_RECORDS *int

var TIMESERIES = false
var f_TIME *bool = &TIMESERIES
var f_TIME_COL *string
var f_TIME_BUCKET *int

var f_PRINT_KEYS *bool = &TIMESERIES
var f_LOAD_AND_QUERY *bool = &FALSE

var f_JSON *bool = &FALSE

var SORT_COUNT = "$COUNT"
var f_DIR = flag.String("dir", "./db/", "Directory to store DB files")
var f_TABLE = flag.String("table", "", "Table to operate on [REQUIRED]")
var f_PRINT_INFO = flag.Bool("info", false, "Print table info")
var f_SORT = flag.String("sort", SORT_COUNT, "Int Column to sort by")
var f_LIMIT = flag.Int("limit", 100, "Number of results to return")

var f_UPDATE_TABLE_INFO = &FALSE
