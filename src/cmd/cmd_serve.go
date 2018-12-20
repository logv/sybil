// +build grpc

package sybil_cmd

import (
	"flag"
	"fmt"
	"net"
	"os"

	sybil "github.com/logv/sybil/src/lib"
	"github.com/logv/sybil/src/sybild"
	pb "github.com/logv/sybil/src/sybild/pb"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

const defaultServeListenAddr = "localhost:7000"

func RunServeCmdLine() {
	flag.Parse()
	if err := runServeCmdLine(&sybil.FLAGS); err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrap(err, "serve"))
		os.Exit(1)
	}
}

func runServeCmdLine(flags *sybil.FlagDefs) error {
	// TODO: handle signals, shutdown
	// TODO: auth, tls
	// TODO: add configurable listening address
	lis, err := net.Listen("tcp", defaultServeListenAddr)
	if err != nil {
		return err
	}
	s, err := sybild.NewServer()
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	pb.RegisterSybilServer(grpcServer, s)
	return grpcServer.Serve(lis)
}
