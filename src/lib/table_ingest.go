package edb

import "fmt"
import "os"

// there exists two dirs for ingesting and digesting:
// ingest/
// digest/

// to ingest, make a new tmp file inside ingest/ (or append to an existing one)
// to digest, move that file into digest/ and begin digesting it
// POTENTIAL RACE CONDITION ON INGEST MODIFYING AN EXISTING FILE!

// Go through newRecords list and save all the new records out to a row store
func (t *Table) IngestRecords(blockname string) {
	fmt.Println("KEY TABLE", t.KeyTable)

	t.AppendRecordsToLog(t.newRecords[:], blockname)

	t.SaveTableInfo("info")
}

// Go through rowstore and save records out to column store
func (t *Table) DigestRecords(digest string) {
	// TODO: REFUSE TO DIGEST IF THE DIGEST AREA ALREADY EXISTS
	filename := fmt.Sprintf("db/%s/ingest/%s.db", t.Name, digest)
	digestname := fmt.Sprintf("db/%s/digest/%s.db", t.Name, digest)
	os.MkdirAll(fmt.Sprintf("db/%s/digest", t.Name), 0777)

	fmt.Println("Moving", filename, "TO", digestname, "FOR DIGESTION")
	err := os.Rename(filename, digestname)
	if err != nil {
		fmt.Println("NO INGEST LOG, ERR:", err)
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
