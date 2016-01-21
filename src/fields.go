package edb

import "sync"

type IntArr []IntField;
type StrArr []StrField;
type SetArr []SetField;

var STRING_LOOKUP = make(map[string]int);


type IntField struct {
  name int;
  value int;
}

var string_id_m = &sync.Mutex{}
func get_string_id(name string) int {
  id, ok := STRING_LOOKUP[name]

  if ok {
    return id;
  }


  string_id_m.Lock();
  STRING_LOOKUP[name] = len(STRING_LOOKUP);
  string_id_m.Unlock();
  return STRING_LOOKUP[name];
}
func NewIntField(name string, value int) IntField {
  name_id := get_string_id(name);

  return IntField{ name_id, value };
}

// value is held in the look up table!
type StrField struct {
  name int;
  value int;
}

func NewStrField(name string, value string) StrField {
  name_id := get_string_id(name);
  value_id := get_string_id(value);

  return StrField{ name_id, value_id };
}

// a set field holds sets of strings
type SetField struct {
  name int;
  value []int;
}

func NewSetField(name string, value []int) SetField {
  name_id := get_string_id(name);

  return SetField{ name_id, value };
}
