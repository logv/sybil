package sybil

import "encoding/gob"
import "flag"
import "log"
import "strconv"
import "os"

func decodeInfoCol(digest_file *string) bool {

	file, err := os.Open(*digest_file)
	dec := gob.NewDecoder(file)
	info := SavedColumnInfo{}
	err = dec.Decode(&info)
	log.Println("INFO", info)

	if err != nil {
		log.Println("ERROR DECODING FILE", *digest_file, err)
		return false
	}

	return true

}

func decodeIntCol(digest_file *string) bool {

	file, err := os.Open(*digest_file)
	dec := gob.NewDecoder(file)
	info := SavedIntColumn{}
	err = dec.Decode(&info)
	log.Println("INFO", info)

	if err != nil {
		log.Println("ERROR DECODING FILE", *digest_file, err)
		return false
	}

	return true

}

func decodeStrCol(digest_file *string) bool {

	file, err := os.Open(*digest_file)
	dec := gob.NewDecoder(file)
	info := SavedStrColumn{}
	err = dec.Decode(&info)
	log.Println("INFO", info)
	bins := make([]string, 0)
	for _, bin := range info.Bins {
		bins = append(bins, strconv.FormatInt(int64(len(bin.Records)), 10))
	}
	log.Println("BINS ARE", bins)

	if err != nil {
		log.Println("ERROR DECODING FILE", *digest_file, err)
		return false
	}

	return true

}

// appends records to our record input queue
// every now and then, we should pack the input queue into a column, though
func RunInspectCmdLine() {
	digest_file := flag.String("file", INGEST_DIR, "Name of file to inspect")
	flag.Parse()

	if *f_PROFILE && PROFILER_ENABLED {
		profile := RUN_PROFILER()
		defer profile.Start().Stop()
	}

	if decodeInfoCol(digest_file) {
		return
	}
	if decodeStrCol(digest_file) {
		return
	}
	if decodeIntCol(digest_file) {
		return
	}

}
