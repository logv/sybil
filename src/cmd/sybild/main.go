package main

import (
	"fmt"
	"os"

	"github.com/logv/sybil/src/sybild"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "sybild:", err)
		os.Exit(1)
	}
}

func run() error {
	s, err := sybild.NewServer()
	if err != nil {
		return err
	}
	_ = s
	return nil
}
