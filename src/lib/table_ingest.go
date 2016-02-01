package edb

import "fmt"
import "os"

// Go through newRecords list and save all the new records out to a row store
func (t *Table) IngestRecords() {
  fmt.Println("KEY TABLE", t.KeyTable)

  t.AppendRecordsToLog(t.newRecords[:])

  t.SaveTableInfo("info")
}

// Go through rowstore and save records out to column store
func (t *Table) DigestRecords() {
  // TODO: REFUSE TO DIGEST IF THE DIGEST AREA ALREADY EXISTS
  filename := fmt.Sprintf("db/%s/ingest.db", t.Name)
  digestname := fmt.Sprintf("db/%s/digesting.db", t.Name)

  fmt.Println("Moving", filename, "TO", digestname, "FOR DIGESTION")
  err := os.Rename(filename, digestname)
  if err != nil {
    fmt.Println("NO INGEST LOG? ERR:", err)
    return
  }

  records := t.LoadRecordsFromLog(digestname)
  fmt.Println("LOADED", len(records), "FOR DIGESTION")

  if len(records) > 0 {
    t.newRecords = records
    t.SaveRecords()
  }

  fmt.Println("Removing", digestname)
  os.Remove(digestname)
}
