package api

import "strconv"
import "os/exec"
import "encoding/json"
import "strings"
import "fmt"
import "io/ioutil"
import "os"

// {{{ QUERIES

var FIELD_SEPARATOR = string([]byte{30})
var FILTER_SEPARATOR = string([]byte{31})
var DEBUG_SYBIL = os.Getenv("DEBUG_SYBIL") != ""

type SybilQuery struct {
	Config *SybilConfig
	Flags  []string

	TimeBucket int
	TimeCol    string

	Strs   []string
	Ints   []string
	Sets   []string
	Floats []string

	IntFilters   []SybilFilter
	StrFilters   []SybilFilter
	SetFilters   []SybilFilter
	FloatFilters []SybilFilter

	Results string

	ReadLog bool // whether the query should read the row store ingestion log
}

func (t *SybilTable) Query() *SybilQuery {
	sq := SybilQuery{}
	sq.Config = t.Config
	sq.Flags = make([]string, 0)
	sq.IntFilters = make([]SybilFilter, 0)
	sq.StrFilters = make([]SybilFilter, 0)
	sq.SetFilters = make([]SybilFilter, 0)
	sq.Strs = make([]string, 0)
	sq.Sets = make([]string, 0)
	sq.Ints = make([]string, 0)

	sq.ReadLog = true

	return &sq
}

// SELECTING QUERY TYPE

func (sq *SybilQuery) TimeSeries(timeCol string, bucket int) *SybilQuery {
	sq.Flags = append(sq.Flags, "-time-bucket", strconv.Itoa(bucket), "-time-col", timeCol)
	return sq

}

func (sq *SybilQuery) ReadRowLog(v bool) *SybilQuery {
	sq.ReadLog = v
	return sq
}

func (sq *SybilQuery) Rollup() *SybilQuery {
	return sq

}

func (sq *SybilQuery) Samples() *SybilQuery {
	sq.Flags = append(sq.Flags, "-samples")
	return sq
}

// SELECTING COLUMNS
func (sq *SybilQuery) Aggregate(field string) *SybilQuery {
	sq.Ints = append(sq.Ints, field)
	return sq
}

func (sq *SybilQuery) Hist() *SybilQuery {
	sq.Flags = append(sq.Flags, "-op", "hist")
	return sq
}

func (sq *SybilQuery) LogHist() *SybilQuery {
	sq.Flags = append(sq.Flags, "-op", "hist", "-loghist")
	return sq

}

func (sq *SybilQuery) GroupBy(field string) *SybilQuery {
	sq.Strs = append(sq.Strs, field)
	return sq
}

func (sq *SybilQuery) WeightCol(field string) *SybilQuery {
	sq.Flags = append(sq.Flags, "-weight-col", field)
	return sq
}

func (sq *SybilQuery) Limit(limit int) *SybilQuery {
	sq.Flags = append(sq.Flags, "-limit", strconv.Itoa(limit))
	return sq
}

// FILTERS
func (sq *SybilQuery) IntFilterEq(field string, value int) *SybilQuery {
	sq.IntFilters = append(sq.IntFilters, SybilFilter{field, "eq", strconv.Itoa(value)})
	return sq
}

func (sq *SybilQuery) IntFilterGt(field string, value int) *SybilQuery {
	sq.IntFilters = append(sq.IntFilters, SybilFilter{field, "gt", strconv.Itoa(value)})
	return sq

}

func (sq *SybilQuery) IntFilterLt(field string, value int) *SybilQuery {
	sq.IntFilters = append(sq.IntFilters, SybilFilter{field, "lt", strconv.Itoa(value)})
	return sq

}

func (sq *SybilQuery) IntFilterNeq(field string, value int) *SybilQuery {
	sq.IntFilters = append(sq.IntFilters, SybilFilter{field, "neq", strconv.Itoa(value)})
	return sq
}

func (sq *SybilQuery) StrFilterEq(field string, value string) *SybilQuery {
	return sq

}

func (sq *SybilQuery) StrFilterRegex(field string, value string) *SybilQuery {
	return sq
}

func buildFilters(filterFlag string, filters []SybilFilter) []string {
	if len(filters) == 0 {
		return []string{}
	}

	allFilters := []string{}
	for _, f := range filters {
		allFilters = append(allFilters, strings.Join([]string{f.Field, f.Op, f.Value}, FILTER_SEPARATOR))
	}

	return []string{filterFlag, strings.Join(allFilters, FIELD_SEPARATOR)}
}

// ACTUALLY RUNNING THE QUERY

func (sq *SybilQuery) Execute() ([]SybilResult, error) {
	flags := []string{"query", "-dir", sq.Config.Dir, "--table", sq.Config.Table, "--json"}

	if sq.ReadLog {
		flags = append(flags, "--read-log")
	}

	flags = append(flags, sq.Flags...)
	flags = append(flags, "--filter-separator", fmt.Sprintf("%s", FILTER_SEPARATOR))
	flags = append(flags, "--field-separator", fmt.Sprintf("%s", FIELD_SEPARATOR))

	flags = append(flags, buildFilters("-str-filter", sq.StrFilters)...)
	flags = append(flags, buildFilters("-int-filter", sq.IntFilters)...)
	flags = append(flags, buildFilters("-set-filter", sq.SetFilters)...)

	if len(sq.Strs) > 0 {
		flags = append(flags, "-group", strings.Join(sq.Strs, FIELD_SEPARATOR))
	}
	if len(sq.Ints) > 0 {
		flags = append(flags, "-int", strings.Join(sq.Ints, FIELD_SEPARATOR))
	}
	if len(sq.Sets) > 0 {
		flags = append(flags, "-set", strings.Join(sq.Sets, FIELD_SEPARATOR))
	}

	cmd := exec.Command(SYBIL_BIN, flags...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		Error(err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		Error(err)
	}

	Debug("RUNNING COMMAND", SYBIL_BIN, flags)

	if err := cmd.Start(); err != nil {
		Error(err)
	}

	slurp, _ := ioutil.ReadAll(stderr)
	out, _ := ioutil.ReadAll(stdout)

	if DEBUG_SYBIL {
		fmt.Printf("STDERR: %s\n", slurp)
	}

	if err != nil {
		Error("CAN'T READ DB INFO FOR", sq.Config.Dir, err)
	} else {
		var unmarshalled []SybilResult
		err := json.Unmarshal(out, &unmarshalled)
		if err != nil {
			Print("COULDN'T READ TABLE LIST FOR", sq.Config.Table, "ERR", err)
		} else {
			return unmarshalled, nil
		}
	}

	return nil, nil
}

// }}} QUERIES
