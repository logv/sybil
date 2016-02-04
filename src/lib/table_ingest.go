package edb

import "log"
import "fmt"
import "os"
import "strings"

// there exists two dirs for ingesting and digesting:
// ingest/
// digest/

// to ingest, make a new tmp file inside ingest/ (or append to an existing one)
// to digest, move that file into digest/ and begin digesting it
// POTENTIAL RACE CONDITION ON INGEST MODIFYING AN EXISTING FILE!

// Go through newRecords list and save all the new records out to a row store
func (t *Table) IngestRecords(blockname string) {
	log.Println("KEY TABLE", t.KeyTable)

	t.AppendRecordsToLog(t.newRecords[:], blockname)
	t.newRecords = make([]*Record, 0)
	t.SaveTableInfo("info")
}

// Go through rowstore and save records out to column store
func (t *Table) DigestRecords(digest string) {
	// TODO: REFUSE TO DIGEST IF THE DIGEST AREA ALREADY EXISTS
	dirname := fmt.Sprintf("%s/%s/ingest/", *f_DIR, t.Name)

	file, err := os.Open(dirname)
	if err != nil {
		log.Println("Can't open the ingestion dir", dirname)
		return
	}

	files, err := file.Readdir(0)
	digestname := fmt.Sprintf("%s/%s/digest/%s.db", *f_DIR, t.Name, digest)
	os.MkdirAll(fmt.Sprintf("%s/%s/digest", *f_DIR, t.Name), 0777)
	for _, filename := range files {

		if strings.HasPrefix(filename.Name(), digest) == false {
			continue
		}
		if strings.HasSuffix(filename.Name(), ".db") == false {
			continue
		}

		log.Println("Moving", filename, "TO", digestname, "FOR DIGESTION")
		fullname := fmt.Sprintf("%s/%s", dirname, filename.Name())

		err := os.Rename(fullname, digestname)
		if err != nil {
			log.Println("NO INGEST LOG, ERR:", err)
			return
		}

		records := t.LoadRecordsFromLog(digestname)
		log.Println("LOADED", len(records), "FOR DIGESTION")

		if len(records) > 0 {
			t.newRecords = records
			t.SaveRecords()
		}

		log.Println("Removing", digestname)
		os.Remove(digestname)

	}

}
