package cmd

import (
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/logv/sybil/src/sybil"
	"github.com/logv/sybil/src/sybild"
	pb "github.com/logv/sybil/src/sybilpb"
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
	//ctx := context.Background()
	// TODO: handle signals, shutdown
	// TODO: auth, tls
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
