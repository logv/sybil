package cmd

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/logv/sybil/src/sybil"
	"github.com/pkg/errors"
)

func RunIndexCmdLine() {
	var fInts = flag.String("int", "", "Integer values to index")
	flag.Parse()
	ints := strings.Split(*fInts, sybil.FLAGS.FIELD_SEPARATOR)
	if err := runIndexCmdLine(&sybil.FLAGS, ints); err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrap(err, "index"))
		os.Exit(1)
	}
}

func runIndexCmdLine(flags *sybil.FlagDefs, ints []string) error {
	if sybil.FLAGS.TABLE == "" {
		flag.PrintDefaults()
		return sybil.ErrMissingTable
	}

	sybil.FLAGS.UPDATE_TABLE_INFO = true

	t := sybil.GetTable(sybil.FLAGS.TABLE)

	t.LoadRecords(nil)
	t.SaveTableInfo("info")
	sybil.FLAGS.WRITE_BLOCK_INFO = true

	loadSpec := t.NewLoadSpec()
	for _, v := range ints {
		err := loadSpec.Int(v)
		if err != nil {
			return err
		}
	}
	t.LoadRecords(&loadSpec)
	t.SaveTableInfo("info")
	return nil
}
