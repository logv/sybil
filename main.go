package main

import (
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/logv/sybil/src/cmd"
)

var (
	cmdFuncs = make(map[string]func())
	cmdKeys  = make([]string, 0)
)

func setupCommands() {
	cmdFuncs["ingest"] = cmd.RunIngestCmdLine
	cmdFuncs["digest"] = cmd.RunDigestCmdLine
	cmdFuncs["trim"] = cmd.RunTrimCmdLine
	cmdFuncs["query"] = cmd.RunQueryCmdLine
	cmdFuncs["index"] = cmd.RunIndexCmdLine
	cmdFuncs["rebuild"] = cmd.RunRebuildCmdLine
	cmdFuncs["inspect"] = cmd.RunInspectCmdLine
	cmdFuncs["aggregate"] = cmd.RunAggregateCmdLine
	cmdFuncs["version"] = cmd.RunVersionCmdLine

	for k := range cmdFuncs {
		cmdKeys = append(cmdKeys, k)
	}
}

var usage = `sybil: a fast and simple NoSQL column store

Commands: ingest, digest, trim, query, session, rebuild, inspect

Storage Commands:

  ingest: ingest records into the row store

    example: sybil ingest -table TABLE < my_record.json
    example: sybil ingest -table TABLE -csv < my_records.csv

  digest: collate row store records into column blocks

    example: sybil digest -table TABLE

  trim: trim a table to fit into a set amount of space or time limit

    example: sybil trim -table TABLE -mb 100 -list
    example: sybil trim -table TABLE -mb 100 -delete

Query Commands:

  query: run aggregation queries on records inside a table

    example: sybil query -table TABLE -info
    example: sybil query -table TABLE -print -group col1 -int col2 -op hist
    # reads the row store log (off by default)
    example: sybil query -table TABLE -read-log -print -group col1 -int col2 -op hist

Emergency Maintenance Commands:

  rebuild: re-create the main table info.db based on the consensus of blocks' info.db

    example: sybil rebuild -table TABLE

  inspect: examine sybil .db files

    example: sybil inspect -file ./db/TABLE/info.db
    example: sybil inspect -file ./db/TABLE/BLOCK/info.db
    example: sybil inspect -file ./db/TABLE/BLOCK/str_COL.db

`

func printCommandHelp(msg string) {
	sort.Strings(cmdKeys)

	fmt.Print(usage)
	log.Fatal(msg)
}

func main() {
	setupCommands()

	if len(os.Args) < 2 {
		printCommandHelp("insufficient number of arguments")
	}

	firstArg := os.Args[1]
	os.Args = os.Args[1:]

	handler, ok := cmdFuncs[firstArg]
	if !ok {
		printCommandHelp(fmt.Sprintf("subcommand '%s' is invalid", firstArg))
	}

	handler()

}
