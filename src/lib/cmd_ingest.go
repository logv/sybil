package edb

import (
	"encoding/json"
	"flag"
	"strconv"
	"io/ioutil"
	"path"
	"fmt"
	"io"
	"strings"
	"log"
	"os"
)

type Dictionary map[string]interface{}

func (t *Table) getNewIngestBlockName() (string, error) {
	name, err := ioutil.TempDir(path.Join(*f_DIR, t.Name), "ingest")
	return name, err
}

func ingest_dictionary(r *Record, recordmap *Dictionary, prefix string) {
	for k, v := range *recordmap {
		key_name := fmt.Sprintf("%s%s", prefix, k)
		prefix_name := fmt.Sprintf("%s.", key_name)
		switch iv := v.(type) {
		case string:
			if INT_CAST[key_name] == true {
				val, err := strconv.ParseInt(iv, 10, 64)
				if err != nil {
					r.AddIntField(key_name, int(val))
				}
			} else {
				r.AddStrField(key_name, iv)

			}
		case float64:
			if STR_CAST[key_name] == true {
				r.AddStrField(key_name, fmt.Sprint(iv))
			} else {
				r.AddIntField(key_name, int(iv))
			}
		case map[string]interface{}:
			d := Dictionary(iv)
			ingest_dictionary(r, &d, prefix_name) 
		case Dictionary:
			ingest_dictionary(r, &iv, prefix_name)
		}
	}
}


var INT_CAST = make(map[string]bool)
var STR_CAST = make(map[string]bool)
// appends records to our record input queue
// every now and then, we should pack the input queue into a column, though
func RunIngestCmdLine() {
	ingestfile := flag.String("file", "ingest", "name of dir to ingest into")
	f_INTS := flag.String("ints", "", "columns to treat as ints (comma delimited)")
	f_STRS := flag.String("strs", "", "columns to treat as strings (comma delimited)")


	flag.Parse()

	digestfile := fmt.Sprintf("%s", *ingestfile)

	if *f_TABLE == "" {
		flag.PrintDefaults()
		return
	}
	if *f_PROFILE && PROFILER_ENABLED {
		profile := RUN_PROFILER()
		defer profile.Start().Stop()
	}


	for _, v := range strings.Split(*f_STRS, ",") {
		STR_CAST[v] = true
	}
	for _, v := range strings.Split(*f_INTS, ",") {
		INT_CAST[v] = true
	}


	t := GetTable(*f_TABLE)
	t.LoadRecords(nil)

	dec := json.NewDecoder(os.Stdin)
	count := 0
	for {
		var recordmap Dictionary

		if err := dec.Decode(&recordmap); err != nil {
			if err == io.EOF {
				break
			}

			log.Println("ERR:", err)

			continue
		}

		r := t.NewRecord()

		ingest_dictionary(r, &recordmap, "")

		count++

		if count >= CHUNK_SIZE {
			count -= CHUNK_SIZE

			os.MkdirAll(path.Join(*f_DIR, t.Name), 0777)
			name, err := t.getNewIngestBlockName()
			if err == nil {
				t.SaveRecordsToBlock(t.newRecords, name)
				t.SaveTableInfo("info")
				t.newRecords = t.newRecords[:0]
				t.ReleaseRecords()
			} else {
				log.Fatal("ERROR SAVING BLOCK", err)
			}
		}

	}

	t.IngestRecords(digestfile)
}
