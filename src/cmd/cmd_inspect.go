package sybilCmd

import sybil "github.com/logv/sybil/src/lib"

import "flag"

import "strconv"

func decodeTableInfo(digestFile *string) bool {
	dec := sybil.GetFileDecoder(*digestFile)

	savedTable := sybil.Table{}
	err := dec.Decode(&savedTable)

	if err != nil || len(savedTable.KeyTable) == 0 {
		return false
	}

	sybil.Print("TABLE INFO", savedTable)

	return true

}

func decodeInfoCol(digestFile *string) bool {
	dec := sybil.GetFileDecoder(*digestFile)

	info := sybil.SavedColumnInfo{}
	err := dec.Decode(&info)

	if err != nil {
		sybil.Print("ERROR", err)
		return false
	}

	sybil.Print("INFO COL", info)

	return true

}

func decodeIntCol(digestFile *string) bool {
	dec := sybil.GetFileDecoder(*digestFile)

	info := sybil.SavedIntColumn{}
	err := dec.Decode(&info)

	if err != nil {
		sybil.Print("ERROR", err)
		return false
	}

	sybil.Print("INT COL", info)

	return true

}

func decodeStrCol(digestFile *string) bool {
	dec := sybil.GetFileDecoder(*digestFile)

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
	digestFile := flag.String("file", "", "Name of file to inspect")
	flag.Parse()

	if *digestFile == "" || digestFile == nil {
		sybil.Print("Please specify a file to inspect with the -file flag")
		return
	}

	if decodeTableInfo(digestFile) {
		return
	}

	if decodeInfoCol(digestFile) {
		return
	}
	if decodeStrCol(digestFile) {
		return
	}
	if decodeIntCol(digestFile) {
		return
	}

}
