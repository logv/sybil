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

	t := sybil.GetTable(*flags.DIR, *flags.TABLE)

	loadSpec := t.NewLoadSpec()
	loadSpec.WriteBlockInfo = true
	t.LoadRecords(&loadSpec)
	t.SaveTableInfo("info")

	loadSpec = t.NewLoadSpec()
	for _, v := range ints {
		loadSpec.Int(v)
	}
	t.LoadRecords(&loadSpec)
	t.SaveTableInfo("info")
}
