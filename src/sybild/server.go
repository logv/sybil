package sybild

import (
	"encoding/json"
	"os"

	context "golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

func (s *Server) Ingest(ctx context.Context, r *pb.IngestRequest) (*pb.IngestResponse, error) {
	json.NewEncoder(os.Stdout).Encode(r)
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *Server) Query(ctx context.Context, r *pb.QueryRequest) (*pb.QueryResponse, error) {
	json.NewEncoder(os.Stdout).Encode(r)
	// TODO QueryRequest to FLAGS (racily), loadSpec, querySpec
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// ListTables .
func (s *Server) ListTables(ctx context.Context, r *pb.ListTablesRequest) (*pb.ListTablesResponse, error) {
	json.NewEncoder(os.Stdout).Encode(r)
	tables, err := s.db.ListTables()
	return &pb.ListTablesResponse{
		Tables: tables,
	}, err
}

func (s *Server) GetTable(ctx context.Context, r *pb.GetTableRequest) (*pb.Table, error) {
	json.NewEncoder(os.Stdout).Encode(r)
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *Server) Trim(ctx context.Context, r *pb.TrimRequest) (*pb.TrimResponse, error) {
	json.NewEncoder(os.Stdout).Encode(r)
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
