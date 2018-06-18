package sybil

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// This is the passed in flags
type FilterSpec struct {
	Int string
	Str string
	Set string
}

func checkTable(tokens []string, t *Table) bool {
	if len(tokens) > 3 {
		return t.Name == tokens[3]
	}
	return true
}

func BuildFilters(t *Table, loadSpec *LoadSpec, filterSpec FilterSpec) ([]Filter, error) {
	strfilters := make([]string, 0)
	intfilters := make([]string, 0)
	setfilters := make([]string, 0)
	if filterSpec.Int != "" {
		intfilters = strings.Split(filterSpec.Int, FLAGS.FIELD_SEPARATOR)
	}
	if filterSpec.Str != "" {
		strfilters = strings.Split(filterSpec.Str, FLAGS.FIELD_SEPARATOR)
	}

	if filterSpec.Set != "" {
		setfilters = strings.Split(filterSpec.Set, FLAGS.FIELD_SEPARATOR)
	}

	filters := []Filter{}

	for _, filt := range intfilters {
		tokens := strings.Split(filt, FLAGS.FILTER_SEPARATOR)
		col := tokens[0]
		op := tokens[1]
		val, _ := strconv.ParseInt(tokens[2], 10, 64)

		if !checkTable(tokens, t) {
			continue
		}

		// we align the Time Filter to the Time Bucket iff we are doing a time series query
		if col == FLAGS.TIME_COL && FLAGS.TIME {
			bucket := int64(FLAGS.TIME_BUCKET)
			newVal := int64(val/bucket) * bucket

			if val != newVal {
				Debug("ALIGNING TIME FILTER TO BUCKET", val, newVal)
				val = newVal
			}
		}

		filters = append(filters, t.IntFilter(col, op, int(val)))
		if err := loadSpec.Int(col); err != nil {
			return nil, errors.Wrap(err, "building int filters")
		}
	}

	for _, filter := range setfilters {
		tokens := strings.Split(filter, FLAGS.FILTER_SEPARATOR)
		col := tokens[0]
		op := tokens[1]
		val := tokens[2]

		if !checkTable(tokens, t) {
			continue
		}
		if err := loadSpec.Set(col); err != nil {
			return nil, errors.Wrap(err, "building set filters")
		}

		filters = append(filters, t.SetFilter(col, op, val))

	}

	for _, filter := range strfilters {
		tokens := strings.Split(filter, FLAGS.FILTER_SEPARATOR)
		col := tokens[0]
		op := tokens[1]
		val := tokens[2]

		if !checkTable(tokens, t) {
			continue
		}

		if err := loadSpec.Str(col); err != nil {
			return nil, errors.Wrap(err, "building string filters")
		}

		filters = append(filters, t.StrFilter(col, op, val))

	}

	return filters, nil
}

// FILTERS RETURN TRUE ON MATCH SUCCESS
type NoFilter struct{}

func (f NoFilter) Filter(r *Record) bool {
	return false
}

type IntFilter struct {
	Field   string
	FieldID int16
	Op      string
	Value   int

	table *Table
}

type StrFilter struct {
	Field   string
	FieldID int16
	Op      string
	Value   string
	regex   *regexp.Regexp

	table *Table
}

type SetFilter struct {
	Field   string
	FieldID int16
	Op      string
	Value   string

	table *Table
}

func (filter IntFilter) Filter(r *Record) bool {
	if r.Populated[filter.FieldID] == 0 {
		return false
	}

	field := r.Ints[filter.FieldID]
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
	if r.Populated[filter.FieldID] == 0 {
		return false
	}

	val := r.Strs[filter.FieldID]
	col := r.block.GetColumnInfo(filter.FieldID)
	filterval := int(col.getValID(filter.Value))

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
				strVal := col.getStringForVal(int32(val))
				ret = filter.regex.MatchString(strVal)
			}
		} else {
			strVal := col.getStringForVal(int32(val))
			ret = filter.regex.MatchString(strVal)
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

	col := r.block.GetColumnInfo(filter.FieldID)
	ret := false

	ok := r.Populated[filter.FieldID] == SET_VAL
	if !ok {
		return false
	}

	sets := r.SetMap[filter.FieldID]

	valID := col.getValID(filter.Value)

	switch filter.Op {
	// Check if tag exists
	case "in":
		// Check if tag does not exist
		for _, tag := range sets {
			if tag == valID {
				return true
			}
		}
	case "nin":
		ret = true
		for _, tag := range sets {
			if tag == valID {
				return false
			}
		}

	}
	return ret
}

func (t *Table) IntFilter(name string, op string, value int) IntFilter {
	intFilter := IntFilter{Field: name, FieldID: t.getKeyID(name), Op: op, Value: value}
	intFilter.table = t

	return intFilter

}

func (t *Table) StrFilter(name string, op string, value string) StrFilter {
	strFilter := StrFilter{Field: name, FieldID: t.getKeyID(name), Op: op, Value: value}
	strFilter.table = t

	var err error
	if op == "re" || op == "nre" {
		strFilter.regex, err = regexp.Compile(value)
		if err != nil {
			Debug("REGEX ERROR", err, "WITH", value)
		}
	}

	return strFilter

}

func (t *Table) SetFilter(name string, op string, value string) SetFilter {
	setFilter := SetFilter{Field: name, FieldID: t.getKeyID(name), Op: op, Value: value}
	setFilter.table = t

	return setFilter

}
