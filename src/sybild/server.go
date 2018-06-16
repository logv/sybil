package sybild

import (
	context "golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/davecgh/go-spew/spew"
	"github.com/logv/sybil/src/sybil"
	pb "github.com/logv/sybil/src/sybilpb"
)

const defaultAddr = ":7000"

// Server implements SybilServer
type Server struct {
	DbDir string

	db *sybil.Database
}

// statically assert that *Server implements SybilServer
var _ pb.SybilServer = (*Server)(nil)

// ServerOption describes options to customize Server implementations.
type ServerOption func(*Server)

func NewServer(opts ...ServerOption) (*Server, error) {
	s := &Server{
		DbDir: "./db",
	}
	for _, o := range opts {
		o(s)
	}
	var err error
	s.db, err = sybil.NewDatabase(s.DbDir)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Server) Ingest(context.Context, *pb.IngestRequest) (*pb.IngestResponse, error) {
	panic("not implemented")
}

func (s *Server) Query(ctx context.Context, r *pb.QueryRequest) (*pb.QueryResponse, error) {
	spew.Dump(r)
	// TODO QueryRequest to FLAGS (racily), loadSpec, querySpec
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *Server) ListTables(context.Context, *pb.ListTablesRequest) (*pb.ListTablesResponse, error) {
	tables, err := s.db.ListTables()
	return &pb.ListTablesResponse{
		Tables: tables,
	}, err
}

func (s *Server) GetTable(context.Context, *pb.GetTableRequest) (*pb.Table, error) {
	panic("not implemented")
}

func (s *Server) Trim(context.Context, *pb.TrimRequest) (*pb.TrimResponse, error) {
	panic("not implemented")
}
