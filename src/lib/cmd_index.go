package sybil

import "flag"
import "strings"

func RunIndexCmdLine() {
	f_STRS = flag.String("str", "", "Str values to index")
	f_INTS = flag.String("int", "", "Integer values to index")
	flag.Parse()
	if *f_TABLE == "" {
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

	f_UPDATE_TABLE_INFO = &TRUE

	t := GetTable(*f_TABLE)

	t.LoadRecords(nil)
	t.SaveTableInfo("info")
	DELETE_BLOCKS_AFTER_QUERY = true
	WRITE_BLOCK_INFO = true

	loadSpec := t.NewLoadSpec()
	for _, v := range strs {
		loadSpec.Str(v)
	}
	for _, v := range ints {
		loadSpec.Int(v)
	}
	t.LoadRecords(&loadSpec)
}
