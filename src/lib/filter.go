package edb

// FILTERS RETURN TRUE ON MATCH SUCCESS
type NoFilter struct{}

func (f NoFilter) Filter(r *Record) bool {
	return false
}

type StrFilter struct {
	Field   string
	FieldId int16
	Op      string
	Value   string

	table *Table
}

type IntFilter struct {
	Field   string
	FieldId int16
	Op      string
	Value   int

	table *Table
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

func (filter StrFilter) Filter(r *Record) bool {
	if r.Populated[filter.FieldId] == 0 {
		return false
	}

	val := r.Strs[filter.FieldId]
	col := r.block.getColumnInfo(filter.FieldId)
	filterval := int(col.get_val_id(filter.Value))

	ret := false
	switch filter.Op {
	case "eq":
		ret = int(val) == filterval

	case "neq":
		ret = int(val) != filterval

	default:

	}

	return ret
}

func (t *Table) IntFilter(name string, op string, value int) IntFilter {
	intFilter := IntFilter{Field: name, FieldId: t.get_key_id(name), Op: op, Value: value}
	intFilter.table = t

	return intFilter

}

func (t *Table) StrFilter(name string, op string, value string) StrFilter {
	strFilter := StrFilter{Field: name, FieldId: t.get_key_id(name), Op: op, Value: value}
	strFilter.table = t

	return strFilter

}
