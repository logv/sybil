package edb

import "flag"

var f_PROFILE  *bool
var f_OP  *string
var f_PRINT  *bool
var f_INT_FILTERS  *string
var f_STR_FILTERS  *string

var f_SESSION_COL  *string
var f_INTS  *string
var f_STRS  *string
var f_GROUPS  *string

var GROUP_BY  []string

var f_ADD_RECORDS *int

var f_TABLE = flag.String("table", "", "Table to operate on [REQUIRED]")
var f_PRINT_INFO = flag.Bool("info", false, "Print table info")
var f_SORT = flag.String("sort", "", "Int Column to sort by")
var f_LIMIT = flag.Int("limit", 100, "Number of results to return")
