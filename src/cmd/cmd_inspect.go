package sybil_cmd

import sybil "github.com/logv/sybil/src/lib"

import "encoding/gob"
import "flag"
import "log"
import "strconv"
import "os"

func decodeTableInfo(digest_file *string) bool {

	saved_table := sybil.Table{}

	file, err := os.Open(*digest_file)
	dec := gob.NewDecoder(file)
	err = dec.Decode(&saved_table)

	if err != nil || len(saved_table.KeyTable) == 0 {
		return false
	}

	log.Println("TABLE INFO", saved_table)

	return true

}

func decodeInfoCol(digest_file *string) bool {

	file, err := os.Open(*digest_file)
	dec := gob.NewDecoder(file)
	info := sybil.SavedColumnInfo{}
	err = dec.Decode(&info)

	if err != nil {
		log.Println("ERROR", err)
		return false
	}

	log.Println("INFO COL", info)

	return true

}

func decodeIntCol(digest_file *string) bool {

	file, err := os.Open(*digest_file)
	dec := gob.NewDecoder(file)
	info := sybil.SavedIntColumn{}
	err = dec.Decode(&info)

	if err != nil {
		log.Println("ERROR", err)
		return false
	}

	log.Println("INT COL", info)

	return true

}

func decodeStrCol(digest_file *string) bool {

	file, err := os.Open(*digest_file)
	dec := gob.NewDecoder(file)
	info := sybil.SavedStrColumn{}
	err = dec.Decode(&info)

	bins := make([]string, 0)
	for _, bin := range info.Bins {
		bins = append(bins, strconv.FormatInt(int64(len(bin.Records)), 10))
	}

	if err != nil {
		log.Println("ERROR", err)
		return false
	}

	log.Println("STR COL", info)
	log.Println("BINS ARE", bins)

	return true

}

// appends records to our record input queue
// every now and then, we should pack the input queue into a column, though
func RunInspectCmdLine() {
	digest_file := flag.String("file", "", "Name of file to inspect")
	flag.Parse()

	if *digest_file == "" || digest_file == nil {
		log.Println("Please specify a file to inspect with the -file flag")
		return
	}

	if decodeTableInfo(digest_file) {
		return
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
