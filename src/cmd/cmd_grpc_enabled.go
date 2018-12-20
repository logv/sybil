// +build grpc

package sybil_cmd

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/ptypes/struct"
	sybil "github.com/logv/sybil/src/lib"
	pb "github.com/logv/sybil/src/sybild/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"os"
	"strings"
)

func splitFilters(s, sep, fSep string) []*pb.QueryFilter {
	if s == "" {
		return nil
	}
	var result []*pb.QueryFilter
	for _, filter := range strings.Split(s, sep) {
		parts := strings.Split(filter, fSep)
		// TODO: check filter validity
		result = append(result, &pb.QueryFilter{
			Column: parts[0],
			Op:     pb.QueryFilterOp(pb.QueryFilterOp_value[strings.ToUpper(parts[1])]),
			Value:  parts[2],
		})
	}
	return result
}
func runIngestGRPC(flags *sybil.FlagDefs, r io.Reader) error {
	ctx := context.Background()
	opts := []grpc.DialOption{
		// todo
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(flags.DIAL, opts...)
	if err != nil {
		return err
	}
	defer conn.Close()
	c := pb.NewSybilClient(conn)
	m := &jsonpb.Marshaler{OrigName: true}
	maxErrs := 100
	var errs int
	var vals []*structpb.Struct
	s := bufio.NewScanner(r)
	for s.Scan() {
		v := &structpb.Struct{}
		if err := jsonpb.Unmarshal(bytes.NewReader(s.Bytes()), v); err != nil {
			if err == io.EOF {
				break
			}
			if err != nil {
				sybil.Debug("ERR", err)
				errs++
				if errs > maxErrs {
					break
				}
				continue
			}
		}
		vals = append(vals, v)
	}
	if err := s.Err(); err != nil {
		return err
	}
	i := &pb.IngestRequest{
		Dataset: flags.TABLE,
		Records: vals,
	}
	qr, err := c.Ingest(ctx, i)
	if err != nil {
		return err
	}
	if err := m.Marshal(os.Stdout, qr); err != nil {
		return err
	}
	if errs > 0 {
		fmt.Fprintln(os.Stderr, "exiting due to error threshold being reached:", errs)
		os.Exit(errs)
	}
	return nil
}

func runQueryGRPC(flags *sybil.FlagDefs) error {
	ctx := context.Background()
	opts := []grpc.DialOption{
		// todo
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(flags.DIAL, opts...)
	if err != nil {
		return err
	}
	defer conn.Close()
	c := pb.NewSybilClient(conn)
	m := &jsonpb.Marshaler{OrigName: true}
	if flags.LIST_TABLES {
		r, err := c.ListTables(ctx, &pb.ListTablesRequest{})
		if err != nil {
			return err
		}
		return m.Marshal(os.Stdout, r)
	}
	if flags.PRINT_INFO {
		r, err := c.GetTable(ctx, &pb.GetTableRequest{
			Name: flags.TABLE,
		})
		if err != nil {
			return err
		}
		return m.Marshal(os.Stdout, r)
	}
	fs, flts := flags.FIELD_SEPARATOR, flags.FILTER_SEPARATOR
	q := &pb.QueryRequest{
		Dataset:          flags.TABLE,
		Ints:             split(flags.INTS, fs),
		Strs:             split(flags.STRS, fs),
		GroupBy:          split(flags.GROUPS, fs),
		DistinctGroupBy:  split(flags.DISTINCT, fs),
		Limit:            int64(flags.LIMIT),
		SortBy:           flags.SORT,
		IntFilters:       splitFilters(flags.INT_FILTERS, fs, flts),
		StrFilters:       splitFilters(flags.STR_FILTERS, fs, flts),
		SetFilters:       splitFilters(flags.SET_FILTERS, fs, flts),
		ReadIngestionLog: flags.READ_ROWSTORE,
		// TODO: replacements
	}
	if flags.SAMPLES {
		q.Type = pb.QueryType_SAMPLES
	}
	if flags.OP == sybil.OP_AVG {
		q.Op = pb.QueryOp_AVERAGE
	} else if flags.OP == sybil.OP_HIST {
		q.Op = pb.QueryOp_HISTOGRAM
		q.Type = pb.QueryType_DISTRIBUTION
	}
	if flags.TIME {
		q.Type = pb.QueryType_TIME_SERIES
	}
	qr, err := c.Query(ctx, q)
	if err != nil {
		return err
	}
	return m.Marshal(os.Stdout, qr)
}
