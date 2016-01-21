package edb

import "fmt"

func Start() {
  fmt.Println("Starting DB");

  r := NewRecord(
    IntArr{ NewIntField("age", 10) },
    StrArr{ NewStrField("name", "okay") },
    SetArr{});

  r = NewRecord(
    IntArr{ NewIntField("age", 20) },
    StrArr{ NewStrField("name", "nokay") },
    SetArr{});


  r = NewRecord(
    IntArr{ NewIntField("age", 20) },
    StrArr{ NewStrField("name", "bokay") },
    SetArr{});

  fmt.Println("Created new record", r);

  PrintRecords()
}
