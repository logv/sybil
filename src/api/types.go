package api

// {{{ TYPES & STRUCTS
var SYBIL_BIN = "sybil"

type SybilConfig struct {
	Dir   string
	Table string
}

type SybilTable struct {
	Config     *SybilConfig
	NewRecords []interface{}
}

type SybilFilter struct {
	Field string
	Op    string
	Value string
}

type SybilRecord struct {
	Sets map[string][]string
	Ints map[string]int
	Strs map[string]string
}

type SybilMapRecord map[string]interface{}

type SybilResult map[string]interface{}

func (sr SybilResult) Int(field string) (int, bool) {
	val, ok := sr[field]
	return int(val.(float64)), ok
}

func (sr SybilResult) Str(field string) (string, bool) {
	val, ok := sr[field]
	return val.(string), ok
}

func (sr SybilResult) Set(field string) (map[string]string, bool) {
	val, ok := sr[field]
	return val.(map[string]string), ok

}

// }}} TYPES & STRUCTS

// {{{ INITIALIZERS
func NewTable(config *SybilConfig) *SybilTable {
	records := make([]interface{}, 0)
	st := SybilTable{config, records}
	return &st
}

func NewRecord() *SybilRecord {
	r := SybilRecord{}
	r.Strs = make(map[string]string)
	r.Ints = make(map[string]int)
	r.Sets = make(map[string][]string)
	return &r
}

// }}}
