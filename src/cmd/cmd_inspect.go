package cmd

import (
	"flag"
	"strconv"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/storage/column_store"
	. "github.com/logv/sybil/src/storage/encoders"
)

func decodeTableInfo(digest_file *string) bool {
	dec := GetFileDecoder(*digest_file)

	saved_table := Table{}
	err := dec.Decode(&saved_table)

	if err != nil || len(saved_table.KeyTable) == 0 {
		return false
	}

	Print("TABLE INFO", saved_table)

	return true

}

func decodeInfoCol(digest_file *string) bool {
	dec := GetFileDecoder(*digest_file)

	info := SavedColumnInfo{}
	err := dec.Decode(&info)

	if err != nil {
		Print("ERROR", err)
		return false
	}

	Print("INFO COL", info)

	return true

}

func decodeIntCol(digest_file *string) bool {
	dec := GetFileDecoder(*digest_file)

	info := SavedIntColumn{}
	err := dec.Decode(&info)

	if err != nil {
		Print("ERROR", err)
		return false
	}

	Print("INT COL", info)

	return true

}

func decodeStrCol(digest_file *string) bool {
	dec := GetFileDecoder(*digest_file)

	info := SavedStrColumn{}
	err := dec.Decode(&info)

	bins := make([]string, 0)
	for _, bin := range info.Bins {
		bins = append(bins, strconv.FormatInt(int64(len(bin.Records)), 10))
	}

	if err != nil {
		Print("ERROR", err)
		return false
	}

	Print("STR COL", info)
	Print("BINS ARE", bins)

	return true

}

// TODO: make a list of potential types that can be decoded into
func RunInspectCmdLine() {
	digest_file := flag.String("file", "", "Name of file to inspect")
	flag.Parse()

	if *digest_file == "" || digest_file == nil {
		Print("Please specify a file to inspect with the -file flag")
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
