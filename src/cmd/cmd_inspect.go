package cmd

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/logv/sybil/src/sybil"
	"github.com/pkg/errors"
)

func decodeTableInfo(digestFile *string) error {
	dec, err := sybil.GetFileDecoder(*digestFile)
	if err != nil {
		return err
	}

	savedTable := sybil.Table{}
	if err = dec.Decode(&savedTable); err != nil {
		return err
	}

	if len(savedTable.KeyTable) == 0 {
		return fmt.Errorf("empty keytable")
	}

	sybil.Print("TABLE INFO", savedTable)

	return nil
}

func decodeInfoCol(digestFile *string) error {
	dec, err := sybil.GetFileDecoder(*digestFile)
	if err != nil {
		return err
	}

	info := sybil.SavedColumnInfo{}
	if err := dec.Decode(&info); err != nil {
		return err
	}

	sybil.Print("INFO COL", info)

	return nil
}

func decodeIntCol(digestFile *string) error {
	dec, err := sybil.GetFileDecoder(*digestFile)
	if err != nil {
		return err
	}

	info := sybil.SavedIntColumn{}
	if err := dec.Decode(&info); err != nil {
		return err
	}

	sybil.Print("INT COL", info)

	return nil
}

func decodeStrCol(digestFile *string) error {
	dec, err := sybil.GetFileDecoder(*digestFile)
	if err != nil {
		return err
	}

	info := sybil.SavedStrColumn{}
	if err := dec.Decode(&info); err != nil {
		return err
	}

	bins := make([]string, 0)
	for _, bin := range info.Bins {
		bins = append(bins, strconv.FormatInt(int64(len(bin.Records)), 10))
	}

	sybil.Print("STR COL", info)
	sybil.Print("BINS ARE", bins)

	return nil
}

// TODO: make a list of potential types that can be decoded into
func RunInspectCmdLine() {
	digestFile := flag.String("file", "", "Name of file to inspect")
	flag.Parse()

	if *digestFile == "" {
		sybil.Print("Please specify a file to inspect with the -file flag")
		return
	}
	if err := runInspectCmdLine(digestFile); err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrap(err, "query"))
		os.Exit(1)
	}
}

func runInspectCmdLine(digestFile *string) error {
	err := decodeTableInfo(digestFile)
	if err == nil {
		return nil
	} else {
		sybil.Debug("inspect encountered:", err)
	}

	err = decodeInfoCol(digestFile)
	if err == nil {
		return nil
	} else {
		sybil.Debug("inspect encountered:", err)
	}

	err = decodeStrCol(digestFile)
	if err == nil {
		return nil
	} else {
		sybil.Debug("inspect encountered:", err)
	}

	err = decodeIntCol(digestFile)
	if err == nil {
		return nil
	} else {
		sybil.Debug("inspect encountered:", err)
	}

	return fmt.Errorf("no decoding succeeded")
}
