package cmd

import (
	"flag"
	"strings"

	sybil "github.com/logv/sybil/src/lib"
	"github.com/logv/sybil/src/lib/config"
)

func RunIndexCmdLine() {
	var f_INTS = flag.String("int", "", "Integer values to index")
	flag.Parse()
	if *config.FLAGS.TABLE == "" {
		flag.PrintDefaults()
		return
	}

	var ints []string
	if *f_INTS != "" {
		ints = strings.Split(*f_INTS, *config.FLAGS.FIELD_SEPARATOR)
	}

	config.FLAGS.UPDATE_TABLE_INFO = &config.TRUE

	t := sybil.GetTable(*config.FLAGS.TABLE)

	t.LoadRecords(nil)
	t.SaveTableInfo("info")
	sybil.DELETE_BLOCKS_AFTER_QUERY = true
	config.OPTS.WRITE_BLOCK_INFO = true

	loadSpec := t.NewLoadSpec()
	for _, v := range ints {
		loadSpec.Int(v)
	}
	t.LoadRecords(&loadSpec)
	t.SaveTableInfo("info")
}
