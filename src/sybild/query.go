package sybild

import (
	"fmt"
	"strings"

	"github.com/logv/sybil/src/sybil"
	pb "github.com/logv/sybil/src/sybilpb"
)

var opsToSybilOps = map[pb.QueryOp]sybil.Op{
	pb.QueryOp_QUERY_OP_UNKNOWN: sybil.NO_OP,
	pb.QueryOp_AVERAGE:          sybil.OP_AVG,
	pb.QueryOp_HISTOGRAM:        sybil.OP_HIST,
}

func queryToSpecs(t *sybil.Table, r *pb.QueryRequest) (*sybil.LoadSpec, *sybil.QuerySpec, error) {
	t.LoadTableInfo()
	//return nil,nil,err
	t.LoadRecords(nil)
	loadSpec := t.NewLoadSpec()
	params := sybil.QueryParams{
		Limit: int(r.Limit),
	}
	for _, v := range r.Strs {
		loadSpec.Str(v)
	}
	for _, v := range r.Ints {
		loadSpec.Int(v)
		params.Aggregations = append(params.Aggregations, t.Aggregation(v, opsToSybilOps[r.Op]))
	}
	for _, v := range r.GroupBy {
		params.Groups = append(params.Groups, t.Grouping(v))
		switch t.GetColumnType(v) {
		case sybil.STR_VAL:
			loadSpec.Str(v)
		case sybil.INT_VAL:
			loadSpec.Int(v)
		default:
			return nil, nil, fmt.Errorf("unknown type %v", t.GetColumnType(v))
		}
	}
	for _, v := range r.DistinctGroupBy {
		params.Distincts = append(params.Distincts, t.Grouping(v))
		switch t.GetColumnType(v) {
		case sybil.STR_VAL:
			loadSpec.Str(v)
		case sybil.INT_VAL:
			loadSpec.Int(v)
		default:
			return nil, nil, fmt.Errorf("unknown type %v", t.GetColumnType(v))
		}
	}

	querySpec := &sybil.QuerySpec{QueryParams: params}
	return &loadSpec, querySpec, nil
}

func querySpecResultsToResults(qr *pb.QueryRequest, r sybil.ResultMap) *pb.QueryResponse {
	results := make([]*pb.QueryResult, 0)
	for _, result := range r {
		qresult := &pb.QueryResult{
			Count:   result.Count,
			Samples: result.Samples,
			Values:  make(map[string]*pb.FieldValue),
		}
		for field, hist := range result.Hists {
			if qr.Op == pb.QueryOp_AVERAGE {
				qresult.Values[field] = &pb.FieldValue{
					Value: &pb.FieldValue_Avg{
						Avg: hist.Mean(),
					},
				}
			} else {
				qresult.Values[field] = &pb.FieldValue{
					Value: &pb.FieldValue_Hist{
						Hist: &pb.Histogram{
							Mean: hist.Mean(),
						},
					},
				}

			}
		}
		var groupKey = strings.Split(result.GroupByKey, sybil.GROUP_DELIMITER)
		for i, g := range qr.GroupBy {
			fmt.Println(g, "=", groupKey[i])
			qresult.Values[g] = &pb.FieldValue{
				Value: &pb.FieldValue_Str{
					Str: groupKey[i],
				},
			}
		}
		fmt.Println(qresult)
		results = append(results, qresult)
		// TODO: needed?
		if qr.Limit > 0 && int64(len(results)) == int64(qr.Limit) {
			continue
		}
	}
	return &pb.QueryResponse{
		Results: results,
	}
}
