package sybild

import (
	"bytes"
	"fmt"
	"strings"

	context "golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/gogo/protobuf/jsonpb"
	sybil "github.com/logv/sybil/src/lib"
	pb "github.com/logv/sybil/src/sybild/pb"
)

// Server implements SybilServer
type Server struct {
	DbDir string
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

	fmt.Println("Serving DB out of", s.DbDir)
	return s, nil
}

func (s *Server) Ingest(ctx context.Context, r *pb.IngestRequest) (*pb.IngestResponse, error) {
	fmt.Println("ingesting", len(r.Records), "records into", r.Dataset)

	buf := new(bytes.Buffer)
	for _, r := range r.Records {
		err := (&jsonpb.Marshaler{}).Marshal(buf, r)

		// TODO: dont error if only one sample is bad
		if err != nil {
			return nil, err
		}
	}

	err := sybilIngest(r.Dataset, buf)
	if err != nil {
		return nil, err
	}

	// TODO: this is presuming success on each record
	return &pb.IngestResponse{
		NumberInserted: int64(len(r.Records)),
	}, nil
}

func (s *Server) Query(ctx context.Context, r *pb.QueryRequest) (*pb.QueryResponse, error) {
	fmt.Println("running query:", r)
	flags := &sybil.FlagDefs{
		OP:                 string(opToSybilOp[r.Op]),
		TABLE:              r.Dataset,
		LIMIT:              int(r.Limit),
		SORT:               r.SortBy,
		TIME:               r.Type == pb.QueryType_TIME_SERIES,
		SAMPLES:            r.Type == pb.QueryType_SAMPLES,
		INTS:               strings.Join(r.Ints, ","),
		STRS:               strings.Join(r.Strs, ","),
		GROUPS:             strings.Join(r.GroupBy, ","),
		DISTINCT:           strings.Join(r.DistinctGroupBy, ","),
		INT_FILTERS:        joinFilters(r.IntFilters),
		STR_FILTERS:        joinFilters(r.StrFilters),
		SET_FILTERS:        joinFilters(r.SetFilters),
		READ_INGESTION_LOG: r.ReadIngestionLog,
		// TODO: add the missing options:
		// query cache control, HistogramOptions,
	}
	results, err := sybilQuery(flags)
	if err != nil {
		return nil, err
	}
	if r.Type == pb.QueryType_SAMPLES {
		return &pb.QueryResponse{
			Samples: convertSamples(results.Samples),
		}, nil
	}

	querySpec := results.QuerySpec
	resp := querySpecResultsToResults(r, querySpec.QueryResults)
	return resp, nil
}

// ListTables .
func (s *Server) ListTables(ctx context.Context, r *pb.ListTablesRequest) (*pb.ListTablesResponse, error) {
	flags := &sybil.FlagDefs{
		DIR:         s.DbDir,
		LIST_TABLES: true,
	}
	results, err := sybilQuery(flags)
	if err != nil {
		return nil, err
	}
	return &pb.ListTablesResponse{
		Tables: results.Tables,
	}, err
}

func (s *Server) GetTable(ctx context.Context, r *pb.GetTableRequest) (*pb.Table, error) {
	fmt.Println("getting table info for", r)
	flags := &sybil.FlagDefs{
		DIR:        s.DbDir,
		PRINT_INFO: true,
		TABLE:      r.Name,
	}
	results, err := sybilQuery(flags)
	if err != nil {
		return nil, err
	}
	t := results.Table
	if err != nil {
		return nil, err
	}
	ci := t.ColInfo()
	return &pb.Table{
		Name:              t.Name,
		StrColumns:        ci.Strs,
		IntColumns:        ci.Ints,
		SetColumns:        ci.Sets,
		Count:             uint64(ci.Count),
		StorageSize:       uint64(ci.Size),
		AverageObjectSize: ci.AverageObjectSize,
	}, nil
}

func (s *Server) Trim(ctx context.Context, r *pb.TrimRequest) (*pb.TrimResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
