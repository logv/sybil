package sybild

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/logv/sybil/src/sybil"
	pb "github.com/logv/sybil/src/sybilpb"

	google_protobuf "github.com/golang/protobuf/ptypes/struct"
)

var defaultFlags = &sybil.FlagDefs{
	TIME_COL:         "time",
	TIME_BUCKET:      3600,
	FIELD_SEPARATOR:  ",",
	FILTER_SEPARATOR: ":",
	LOAD_AND_QUERY:   true,
	RECYCLE_MEM:      true,
	GC:               true,
	DIR:              "./db/",
	SORT:             "$COUNT",
	PRUNE_BY:         "$COUNT",
	SKIP_OUTLIERS:    true,
}

func setDefaults(flags *sybil.FlagDefs) {
	if flags.TIME_COL == "" {
		flags.TIME_COL = defaultFlags.TIME_COL
	}
	if flags.TIME_BUCKET == 0 {
		flags.TIME_BUCKET = defaultFlags.TIME_BUCKET
	}
	if flags.FIELD_SEPARATOR == "" {
		flags.FIELD_SEPARATOR = defaultFlags.FIELD_SEPARATOR
	}
	if flags.FILTER_SEPARATOR == "" {
		flags.FILTER_SEPARATOR = defaultFlags.FILTER_SEPARATOR
	}
	if flags.DIR == "" {
		flags.DIR = defaultFlags.DIR
	}
	if flags.SORT == "" {
		flags.SORT = defaultFlags.SORT
	}
	if flags.PRUNE_BY == "" {
		flags.PRUNE_BY = defaultFlags.PRUNE_BY
	}
	flags.LOAD_AND_QUERY = defaultFlags.LOAD_AND_QUERY
	flags.RECYCLE_MEM = defaultFlags.RECYCLE_MEM
	flags.GC = defaultFlags.GC
	flags.SKIP_OUTLIERS = defaultFlags.SKIP_OUTLIERS
}

func callSybil(flags *sybil.FlagDefs) (*sybil.NodeResults, error) {
	setDefaults(flags)
	const sybilBinary = "sybil"
	var sybilFlags = []string{"query", "-decode-flags", "-encode-results"}
	c := exec.Command(sybilBinary, sybilFlags...)
	c.Stderr = os.Stderr
	si, err := c.StdinPipe()
	if err != nil {
		return nil, err
	}
	if err := gob.NewEncoder(si).Encode(flags); err != nil {
		return nil, err
	}
	so, err := c.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := c.Start(); err != nil {
		return nil, err
	}
	results := &sybil.NodeResults{}
	buf := new(bytes.Buffer)
	io.Copy(buf, so)
	if err := gob.NewDecoder(buf).Decode(results); err != nil {
		return nil, err
	}
	if err := c.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

var opToSybilOp = map[pb.QueryOp]sybil.Op{
	pb.QueryOp_QUERY_OP_UNKNOWN: sybil.NO_OP,
	pb.QueryOp_AVERAGE:          sybil.OP_AVG,
	pb.QueryOp_HISTOGRAM:        sybil.OP_HIST,
}

func querySpecResultsToResults(qr *pb.QueryRequest, qresults sybil.QueryResults) *pb.QueryResponse {
	if qr.Type == pb.QueryType_TIME_SERIES {
		return querySpecResultsToTimeResults(qr, qresults)
	}
	results := make([]*pb.QueryResult, 0)
	for _, result := range qresults.Sorted {
		qresult := sybilResultToPbQueryResult(qr, result)

		//fmt.Println(qresult)
		results = append(results, qresult)
		// TODO: needed?
		if qr.Limit > 0 && int64(len(results)) == int64(qr.Limit) {
			break
		}
	}
	return &pb.QueryResponse{
		Results: results,
	}
}

func querySpecResultsToTimeResults(qr *pb.QueryRequest, qresults sybil.QueryResults) *pb.QueryResponse {
	results := make(map[int64]*pb.QueryResults, 0)

	isTopResult := make(map[string]bool)
	sorted := qresults.Sorted
	if len(sorted) > int(qr.Limit) {
		// TODO: 0 limit?
		sorted = sorted[:qr.Limit]
	}
	for _, result := range sorted {
		isTopResult[result.GroupByKey] = true
	}

	for ts, v := range qresults.TimeResults {
		//results[ts] = make([]*pb.QueryResult, 0)
		k := int64(ts)
		results[k] = &pb.QueryResults{}
		for _, r := range v {
			_, ok := isTopResult[r.GroupByKey]
			if !ok {
				continue
			}
			results[k].Results = append(results[k].Results, sybilResultToPbQueryResult(qr, r))
		}
	}
	return &pb.QueryResponse{
		TimeResults: results,
	}
}

func sybilResultToPbQueryResult(qr *pb.QueryRequest, result *sybil.Result) *pb.QueryResult {
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
	return qresult
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
