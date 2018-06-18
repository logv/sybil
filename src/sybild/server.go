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
const defaultLimit = 100

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
	// set defaults:
	if r.Op == pb.QueryOp_QUERY_OP_UNKNOWN {
		r.Op = pb.QueryOp_AVERAGE
	}
	t := sybil.GetTable(r.Dataset)
	if t.IsNotExist() {
		return nil, status.Error(codes.NotFound, "table not found")
	}
	loadSpec, querySpec, err := queryToSpecs(t, r)
	if err != nil {
		return nil, err
	}
	_, err = t.LoadAndQueryRecords(loadSpec, querySpec)
	if err != nil {
		return nil, err
	}
	if r.Type == pb.QueryType_SAMPLES {
		limit := r.Limit
		if limit == 0 {
			limit = defaultLimit
		}
		samples, err := t.LoadSamples(int(limit))
		if err != nil {
			return nil, err
		}
		return &pb.QueryResponse{
			Samples: convertSamples(samples),
		}, nil
	}
	resp := querySpecResultsToResults(r, querySpec.QueryResults)
	return resp, nil
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
	t, err := s.db.GetTable(r.Name)
	t.LoadRecords(nil)
	if err != nil {
		return nil, err
	}
	ci := t.ColInfo()
	return &pb.Table{
		Name:              t.Name,
		StrColumns:        ci.Strs,
		IntColumns:        ci.Ints,
		SetColumns:        ci.Sets,
		Count:             ci.Count,
		StorageSize:       ci.Size,
		AverageObjectSize: ci.AverageObjectSize,
	}, nil
}

func (s *Server) Trim(ctx context.Context, r *pb.TrimRequest) (*pb.TrimResponse, error) {
	json.NewEncoder(os.Stdout).Encode(r)
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
