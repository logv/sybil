package main

import sybil "github.com/logV/sybil/src/lib"
import "os"
import "log"

func main() {

	if len(os.Args) < 2 {
		log.Fatal("Command should be one of: ingest, digest, query, index")
	}

	first_arg := os.Args[1]
	os.Args = os.Args[1:]
	switch first_arg {
	case "ingest":
		sybil.RunIngestCmdLine()
	case "digest":
		sybil.RunDigestCmdLine()
	case "query":
		sybil.RunQueryCmdLine()
	case "index":
		sybil.RunIndexCmdLine()
	default:
		log.Fatal("Unknown command:", os.Args[0])
	}
}
