package cmd

import (
	"flag"
	"strings"

	"github.com/logv/sybil/src/sybil"
)

func RunIndexCmdLine() {
	var fInts = flag.String("int", "", "Integer values to index")
	flags := sybil.DefaultFlags()
	flag.Parse()
	if *flags.TABLE == "" {
		flag.PrintDefaults()
		return
	}

	var ints []string
	if *fInts != "" {
		ints = strings.Split(*fInts, *flags.FIELD_SEPARATOR)
	}

	flags.UPDATE_TABLE_INFO = sybil.NewTrueFlag()

	t := sybil.GetTable(*flags.TABLE)

	t.LoadRecords(flags, nil)
	t.SaveTableInfo(flags, "info")
	sybil.OPTS.WRITE_BLOCK_INFO = true

	loadSpec := t.NewLoadSpec()
	for _, v := range ints {
		loadSpec.Int(v)
	}
	t.LoadRecords(flags, &loadSpec)
	t.SaveTableInfo(flags, "info")
}
