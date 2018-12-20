// +build !grpc

package sybil_cmd

import sybil "github.com/logv/sybil/src/lib"

func RunServeCmdLine() {
	sybil.Print("sybil must be compiled with +grpc to enable the sybild server")
}
