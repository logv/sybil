package cmd

import (
	"flag"
	"strings"

	. "github.com/logv/sybil/src/lib/column_store"
	"github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/specs"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/lib/table_info"
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

	t := GetTable(*config.FLAGS.TABLE)

	LoadRecords(t, nil)
	SaveTableInfo(t, "info")
	DELETE_BLOCKS_AFTER_QUERY = true
	config.OPTS.WRITE_BLOCK_INFO = true

	loadSpec := NewTableLoadSpec(t)
	for _, v := range ints {
		loadSpec.Int(v)
	}
	LoadRecords(t, &loadSpec)
	SaveTableInfo(t, "info")
}
