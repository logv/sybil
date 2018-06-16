package main

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"

	"github.com/logv/sybil/src/sybil"
)

func init() {
	gob.Register(sybil.FlagDefs{})
}

func main() {
	if err := gob2json(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func gob2json() error {
	v := sybil.FlagDefs{}
	if err := gob.NewDecoder(os.Stdin).Decode(&v); err != nil {
		return err
	}
	if err := json.NewEncoder(os.Stdout).Encode(v); err != nil {
		return err
	}
	return nil
}
