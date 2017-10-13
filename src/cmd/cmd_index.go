package sybilCmd

import sybil "github.com/logv/sybil/src/lib"
import "flag"
import "strings"

func RunIndexCmdLine() {
	var fINTS = flag.String("int", "", "Integer values to index")
	flag.Parse()
	if *sybil.FLAGS.Table == "" {
		flag.PrintDefaults()
		return
	}

	var ints []string
	if *fINTS != "" {
		ints = strings.Split(*fINTS, *sybil.FLAGS.FieldSeparator)
	}

	sybil.FLAGS.UpdateTableInfo = &TRUE

	t := sybil.GetTable(*sybil.FLAGS.Table)

	t.LoadRecords(nil)
	t.SaveTableInfo("info")
	sybil.DeleteBlocksAfterQuery = true
	sybil.OPTS.WriteBlockInfo = true

	loadSpec := t.NewLoadSpec()
	for _, v := range ints {
		loadSpec.Int(v)
	}
	t.LoadRecords(&loadSpec)
	t.SaveTableInfo("info")
}
