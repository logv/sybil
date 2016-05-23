package main

import sybil "github.com/logV/sybil/src/lib"
import cmd "github.com/logV/sybil/src/cmd"
import "os"
import "log"
import "sort"

var CMD_FUNCS = make(map[string]func())
var CMD_KEYS = make([]string, 0)

func setupCommands() {
	CMD_FUNCS["ingest"] = cmd.RunIngestCmdLine
	CMD_FUNCS["digest"] = cmd.RunDigestCmdLine
	CMD_FUNCS["session"] = cmd.RunSessionizeCmdLine
	CMD_FUNCS["expire"] = cmd.RunTrimCmdLine
	CMD_FUNCS["trim"] = cmd.RunTrimCmdLine
	CMD_FUNCS["query"] = cmd.RunQueryCmdLine
	CMD_FUNCS["index"] = cmd.RunIndexCmdLine
	CMD_FUNCS["rebuild"] = cmd.RunIndexCmdLine
	CMD_FUNCS["inspect"] = cmd.RunInspectCmdLine

	for k, _ := range CMD_FUNCS {
		CMD_KEYS = append(CMD_KEYS, k)
	}
}

func printCommandHelp() {
	sort.Strings(CMD_KEYS)
	log.Fatal("Command should be one of: ", CMD_KEYS)
}

func main() {
	setupCommands()

	if len(os.Args) < 2 {
		printCommandHelp()
	}

	first_arg := os.Args[1]
	os.Args = os.Args[1:]

	sybil.SetDefaults()

	handler, ok := CMD_FUNCS[first_arg]
	if !ok {
		printCommandHelp()
	}

	handler()

}
