package sybil

import (
	"strconv"
	"strings"

	. "github.com/logv/sybil/src/lib/common"
	"github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/metadata"
	. "github.com/logv/sybil/src/lib/specs"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/lib/table_column"
)

import "regexp"

// FILTERS RETURN TRUE ON MATCH SUCCESS
type NoFilter struct{}

func (f NoFilter) Filter(r *Record) bool {
	return false
}

type IntFilter struct {
	Field   string
	FieldId int16
	Op      string
	Value   int

	Table *Table
}

type StrFilter struct {
	Field   string
	FieldId int16
	Op      string
	Value   string
	regex   *regexp.Regexp

	Table *Table
}

type SetFilter struct {
	Field   string
	FieldId int16
	Op      string
	Value   string

	Table *Table
}

func (filter IntFilter) Filter(r *Record) bool {
	if r.Populated[filter.FieldId] == 0 {
		return false
	}

	field := r.Ints[filter.FieldId]
	switch filter.Op {
	case "gt":
		return int(field) > int(filter.Value)

	case "lt":
		return int(field) < int(filter.Value)

	case "eq":
		return int(field) == int(filter.Value)

	case "neq":
		return int(field) != int(filter.Value)

	default:

	}

	return false
}

var REGEX_CACHE_SIZE = 100000

func (filter StrFilter) Filter(r *Record) bool {
	if r.Populated[filter.FieldId] == 0 {
		return false
	}

	val := r.Strs[filter.FieldId]
	col := GetColumnInfo(r.Block, filter.FieldId)
	filterval := int(GetColumnValID(col, filter.Value))

	ok := false
	ret := false
	invert := false
	switch filter.Op {
	case "nre":
		invert = true
		fallthrough
	case "re":
		cardinality := len(col.StringTable)
		// we can cache results if the cardinality is reasonably low
		ok = true

		if cardinality < REGEX_CACHE_SIZE {
			ret, ok = col.RCache[int(val)]
			if !ok {
				str_val := GetColumnStringForVal(col, int32(val))
				ret = filter.regex.MatchString(str_val)
			}
		} else {
			str_val := GetColumnStringForVal(col, int32(val))
			ret = filter.regex.MatchString(str_val)
		}

		if cardinality < REGEX_CACHE_SIZE && !ok {
			col.RCache[int(val)] = ret
		}

		if invert {
			ret = !ret
		}

	case "eq":
		ret = int(val) == filterval

	case "neq":
		ret = int(val) != filterval

	default:

	}

	return ret
}

func (filter SetFilter) Filter(r *Record) bool {

	col := GetColumnInfo(r.Block, filter.FieldId)
	ret := false

	ok := r.Populated[filter.FieldId] == SET_VAL
	if !ok {
		return false
	}

	sets := r.SetMap[filter.FieldId]

	val_id := GetColumnValID(col, filter.Value)

	switch filter.Op {
	// Check if tag exists
	case "in":
		// Check if tag does not exist
		for _, tag := range sets {
			if tag == val_id {
				return true
			}
		}
	case "nin":
		ret = true
		for _, tag := range sets {
			if tag == val_id {
				return false
			}
		}

	}
	return ret
}

func MakeIntFilter(t *Table, name string, op string, value int) IntFilter {
	intFilter := IntFilter{Field: name, FieldId: GetTableKeyID(t, name), Op: op, Value: value}

	return intFilter

}

func MakeStrFilter(t *Table, name string, op string, value string) StrFilter {
	strFilter := StrFilter{Field: name, FieldId: GetTableKeyID(t, name), Op: op, Value: value}

	var err error
	if op == "re" || op == "nre" {
		strFilter.regex, err = regexp.Compile(value)
		if err != nil {
			Debug("REGEX ERROR", err, "WITH", value)
		}
	}

	return strFilter

}

func MakeSetFilter(t *Table, name string, op string, value string) SetFilter {
	setFilter := SetFilter{Field: name, FieldId: GetTableKeyID(t, name), Op: op, Value: value}

	return setFilter

}

// This is the passed in flags
type FilterSpec struct {
	Int string
	Str string
	Set string
}

func checkTable(tokens []string, t *Table) bool {
	if len(tokens) > 3 {
		return t.Name == tokens[3]
	} else {
		return true
	}
}

func BuildFilters(t *Table, loadSpec *LoadSpec, filterSpec FilterSpec) []Filter {
	strfilters := make([]string, 0)
	intfilters := make([]string, 0)
	setfilters := make([]string, 0)
	if filterSpec.Int != "" {
		intfilters = strings.Split(filterSpec.Int, *config.FLAGS.FIELD_SEPARATOR)
	}
	if filterSpec.Str != "" {
		strfilters = strings.Split(filterSpec.Str, *config.FLAGS.FIELD_SEPARATOR)
	}

	if filterSpec.Set != "" {
		setfilters = strings.Split(filterSpec.Set, *config.FLAGS.FIELD_SEPARATOR)
	}

	filters := []Filter{}

	for _, filt := range intfilters {
		tokens := strings.Split(filt, *config.FLAGS.FILTER_SEPARATOR)
		col := tokens[0]
		op := tokens[1]
		val, _ := strconv.ParseInt(tokens[2], 10, 64)

		if checkTable(tokens, t) != true {
			continue
		}

		// we align the Time Filter to the Time Bucket iff we are doing a time series query
		if col == *config.FLAGS.TIME_COL && *config.FLAGS.TIME {
			bucket := int64(*config.FLAGS.TIME_BUCKET)
			new_val := int64(val/bucket) * bucket

			if val != new_val {
				Debug("ALIGNING TIME FILTER TO BUCKET", val, new_val)
				val = new_val
			}
		}

		filters = append(filters, MakeIntFilter(t, col, op, int(val)))
		loadSpec.Int(col)
	}

	for _, filter := range setfilters {
		tokens := strings.Split(filter, *config.FLAGS.FILTER_SEPARATOR)
		col := tokens[0]
		op := tokens[1]
		val := tokens[2]

		if checkTable(tokens, t) != true {
			continue
		}
		loadSpec.Set(col)

		filters = append(filters, MakeSetFilter(t, col, op, val))

	}

	for _, filter := range strfilters {
		tokens := strings.Split(filter, *config.FLAGS.FILTER_SEPARATOR)
		col := tokens[0]
		op := tokens[1]
		val := tokens[2]

		if checkTable(tokens, t) != true {
			continue
		}

		loadSpec.Str(col)

		filters = append(filters, MakeStrFilter(t, col, op, val))

	}

	return filters

}
