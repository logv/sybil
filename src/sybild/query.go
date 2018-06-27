package sybild

import (
	"fmt"
	"log"
	"strings"

	"github.com/logv/sybil/src/sybil"
	pb "github.com/logv/sybil/src/sybilpb"

	google_protobuf "github.com/golang/protobuf/ptypes/struct"
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
	params := sybil.QueryParams{
		Limit:   int(r.Limit),
		Samples: r.Type == pb.QueryType_SAMPLES,
	}
	if r.Type == pb.QueryType_DISTRIBUTION {
		params.HistogramParameters.Type = sybil.HistogramTypeBasic
		// todo other params
	}
	loadSpec := t.NewLoadSpec()
	if params.Samples {
		sybil.HOLD_MATCHES = true
		loadSpec.LoadAllColumns = true
		loadSpec.SkipDeleteBlocksAfterQuery = true
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

func querySpecResultsToResults(qr *pb.QueryRequest, qresults sybil.QueryResults) *pb.QueryResponse {
	results := make([]*pb.QueryResult, 0)
	r := qresults.Results
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
						Hist: sybilHistToPbHist(hist),
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

func sybilHistToPbHist(h sybil.Histogram) *pb.Histogram {
	r := &pb.Histogram{
		Mean:         h.Mean(),
		StdDeviation: h.StdDev(),
		Buckets:      h.GetIntBuckets(),
		Percentiles:  h.GetPercentiles(),
	}
	return r
}

func toString(v string) *google_protobuf.Value {
	return &google_protobuf.Value{
		Kind: &google_protobuf.Value_StringValue{
			StringValue: v,
		},
	}
}

func toNumber(v float64) *google_protobuf.Value {
	return &google_protobuf.Value{
		Kind: &google_protobuf.Value_NumberValue{
			NumberValue: v,
		},
	}
}

func toValue(v interface{}) *google_protobuf.Value {
	switch t := v.(type) {
	case int:
		return toNumber(float64(t))
	case float64:
		return toNumber(t)
	case string:
		return toString(t)
	case sybil.IntField:
		return toNumber(float64(t))
	case sybil.StrField:
		return toString(string(t))
	default:
		log.Printf("toValue: unknown type: %T\n", t)
	}
	return nil
}

func convertSamples(samples []*sybil.Sample) []*google_protobuf.Struct {
	result := make([]*google_protobuf.Struct, 0, len(samples))

	for _, s := range samples {
		sv := &google_protobuf.Struct{
			Fields: make(map[string]*google_protobuf.Value),
		}
		for k, v := range *s {
			val := toValue(v)
			if val != nil {
				sv.Fields[k] = toValue(v)
			} else {
				log.Println("got nil from tovalue for", k)
			}
		}
		result = append(result, sv)
	}
	return result
}
