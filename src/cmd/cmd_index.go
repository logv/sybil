package cmd

import (
	"flag"
	"strings"

	"github.com/logv/sybil/src/sybil"
)

func RunIndexCmdLine() {
	var fInts = flag.String("int", "", "Integer values to index")
	flag.Parse()
	if *sybil.FLAGS.TABLE == "" {
		flag.PrintDefaults()
		return
	}

	var ints []string
	if *fInts != "" {
		ints = strings.Split(*fInts, *sybil.FLAGS.FIELD_SEPARATOR)
	}

	sybil.FLAGS.UPDATE_TABLE_INFO = sybil.NewTrueFlag()

	t := sybil.GetTable(*sybil.FLAGS.TABLE)

	t.LoadRecords(nil)
	t.SaveTableInfo("info")
	sybil.OPTS.WRITE_BLOCK_INFO = true

	loadSpec := t.NewLoadSpec()
	for _, v := range ints {
		loadSpec.Int(v)
	}
	t.LoadRecords(&loadSpec)
	t.SaveTableInfo("info")
}
