package edb

type SavedInt struct {
  Name int;
  Value int;
}

type SavedStr struct {
  Name int;
  Value int;

}

type SavedSet struct {
  Name int;
  Value []int;
}

type SavedRecord struct {
  Ints []SavedInt
  Strs []SavedStr
  Sets []SavedSet
}


func (s SavedRecord) toRecord() *Record {
  r := Record{}
  r.Ints = make(map[int]IntField)
  r.Strs = make(map[int]StrField)
  r.Sets = make(map[int]SetField)

  for _, v := range s.Ints {
    r.Ints[v.Name] = IntField(v.Value);
  }

  for _, v := range s.Strs {
    r.Strs[v.Name] = StrField(v.Value);
  }

  for _, v := range s.Sets {
    r.Sets[v.Name] = v.Value
  }


  return &r

}

func (r Record) toSavedRecord() *SavedRecord {
  s := SavedRecord{}
  for k, v := range r.Ints {
    s.Ints = append(s.Ints, SavedInt{k, int(v)})
  }

  for k, v := range r.Strs {
    s.Strs = append(s.Strs, SavedStr{k, int(v)})
  }

  for k, v := range r.Sets {
    s.Sets = append(s.Sets, SavedSet{k, v})
  }

  return &s;

}
