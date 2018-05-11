package cmd

import sybil "github.com/logv/sybil/src/lib"

import "flag"

import "strconv"

func decodeTableInfo(digest_file *string) bool {
	dec := sybil.GetFileDecoder(*digest_file)

	saved_table := sybil.Table{}
	err := dec.Decode(&saved_table)

	if err != nil || len(saved_table.KeyTable) == 0 {
		return false
	}

	sybil.Print("TABLE INFO", saved_table)

	return true

}

func decodeInfoCol(digest_file *string) bool {
	dec := sybil.GetFileDecoder(*digest_file)

	info := sybil.SavedColumnInfo{}
	err := dec.Decode(&info)

	if err != nil {
		sybil.Print("ERROR", err)
		return false
	}

	sybil.Print("INFO COL", info)

	return true

}

func decodeIntCol(digest_file *string) bool {
	dec := sybil.GetFileDecoder(*digest_file)

	info := sybil.SavedIntColumn{}
	err := dec.Decode(&info)

	if err != nil {
		sybil.Print("ERROR", err)
		return false
	}

	sybil.Print("INT COL", info)

	return true

}

func decodeStrCol(digest_file *string) bool {
	dec := sybil.GetFileDecoder(*digest_file)

	info := sybil.SavedStrColumn{}
	err := dec.Decode(&info)

	bins := make([]string, 0)
	for _, bin := range info.Bins {
		bins = append(bins, strconv.FormatInt(int64(len(bin.Records)), 10))
	}

	if err != nil {
		sybil.Print("ERROR", err)
		return false
	}

	sybil.Print("STR COL", info)
	sybil.Print("BINS ARE", bins)

	return true

}

// TODO: make a list of potential types that can be decoded into
func RunInspectCmdLine() {
	digest_file := flag.String("file", "", "Name of file to inspect")
	flag.Parse()

	if *digest_file == "" || digest_file == nil {
		sybil.Print("Please specify a file to inspect with the -file flag")
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
