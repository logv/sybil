package edb

import "flag"
import "fmt"

// appends records to our record input queue
// every now and then, we should pack the input queue into a column, though
func RunDigestCmdLine() {
    flag.Parse()

    if *f_TABLE == "" { flag.PrintDefaults(); return }

    if *f_PROFILE && PROFILER_ENABLED {
      profile := RUN_PROFILER()
      defer profile.Start().Stop()
    }

    t := getTable(*f_TABLE)
    t.LoadRecords(nil)

    fmt.Println("KEY TABLE", t.KeyTable)

    t.DigestRecords()
}
