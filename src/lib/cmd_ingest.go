package edb

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
)

type Dictionary map[string]interface{}

func ingest_dictionary(r *Record, recordmap *Dictionary, prefix string) {
	for k, v := range *recordmap {
		key_name := fmt.Sprintf("%s%s", prefix, k)
		prefix_name := fmt.Sprintf("%s.", key_name)
		switch iv := v.(type) {
		case string:
			r.AddStrField(key_name, iv)
		case float64:
			r.AddIntField(key_name, int(iv))
		case map[string]interface{}:
			d := Dictionary(iv)
			ingest_dictionary(r, &d, prefix_name) 
		case Dictionary:
			ingest_dictionary(r, &iv, prefix_name)
		}
	}
}

// appends records to our record input queue
// every now and then, we should pack the input queue into a column, though
func RunIngestCmdLine() {
	ingestfile := flag.String("file", "ingest", "name of dir to ingest into")

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

			t.IngestRecords(digestfile)
		}

	}

	t.IngestRecords(digestfile)
}
