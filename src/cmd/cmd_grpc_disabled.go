// +build !grpc

package sybil_cmd

import (
	sybil "github.com/logv/sybil/src/lib"
	"io"
)

func runIngestGRPC(flags *sybil.FlagDefs, r io.Reader) error {
	return nil
}

func runQueryGRPC(flags *sybil.FlagDefs) error {
	return nil
}
