package sybil_cmd

import sybil "github.com/logv/sybil/src/lib"
import "flag"
import "strings"

func RunIndexCmdLine() {
	var f_STRS = flag.String("str", "", "Str values to index")
	var f_INTS = flag.String("int", "", "Integer values to index")
	flag.Parse()
	if *sybil.FLAGS.TABLE == "" {
		flag.PrintDefaults()
		return
	}

	var ints []string
	if *f_INTS != "" {
		ints = strings.Split(*f_INTS, ",")
	}

	var strs []string
	if *f_STRS != "" {
		ints = strings.Split(*f_STRS, ",")
	}

	sybil.FLAGS.UPDATE_TABLE_INFO = &TRUE

	t := sybil.GetTable(*sybil.FLAGS.TABLE)

	t.LoadRecords(nil)
	t.SaveTableInfo("info")
	sybil.DELETE_BLOCKS_AFTER_QUERY = true
	sybil.OPTS.WRITE_BLOCK_INFO = true

	loadSpec := t.NewLoadSpec()
	for _, v := range strs {
		loadSpec.Str(v)
	}
	for _, v := range ints {
		loadSpec.Int(v)
	}
	t.LoadRecords(&loadSpec)
}
