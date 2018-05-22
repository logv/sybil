package sybild

import (
	"context"
	"fmt"

	pb "github.com/logv/sybil/src/sybilpb"
)

type Server struct{}

type ServerOption func(*Server)

func NewServer(opts ...ServerOption) (*Server, error) {
	s := &Server{}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

func (s *Server) Serve() error {
	return fmt.Errorf("not implemented")
}

func (s *Server) Ingest(context.Context, *pb.IngestRequest) (*pb.IngestResponse, error) {
	panic("not implemented")
}

func (s *Server) Query(context.Context, *pb.QueryRequest) (*pb.QueryResponse, error) {
	panic("not implemented")
}

func (s *Server) ListTables(context.Context, *pb.ListTablesRequest) (*pb.ListTablesResponse, error) {
	panic("not implemented")
}

func (s *Server) GetTable(context.Context, *pb.GetTableRequest) (*pb.Table, error) {
	panic("not implemented")
}

var _ pb.SybilServer = (*Server)(nil)
