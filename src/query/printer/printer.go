package sybil

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/structs"

	md "github.com/logv/sybil/src/lib/metadata"
	specs "github.com/logv/sybil/src/query/specs"
)

func PrintJSON(data interface{}) {
	b, err := json.Marshal(data)
	if err == nil {
		os.Stdout.Write(b)
	} else {
		Error("JSON encoding error", err)
	}
}

func printTimeResults(querySpec *specs.QuerySpec) {
	Debug("PRINTING TIME RESULTS")
	Debug("CHECKING SORT ORDER", len(querySpec.Sorted))

	is_top_result := make(map[string]bool)
	for _, result := range querySpec.Sorted {
		is_top_result[result.GroupByKey] = true
	}

	keys := make([]int, 0)

	for k := range querySpec.TimeResults {
		keys = append(keys, k)
	}

	sort.Ints(keys)

	Debug("RESULT COUNT", len(querySpec.TimeResults))
	if *FLAGS.JSON {

		marshalled_results := make(map[string][]ResultJSON)
		for k, v := range querySpec.TimeResults {
			key := strconv.FormatInt(int64(k), 10)
			marshalled_results[key] = make([]ResultJSON, 0)

			if *FLAGS.OP == "distinct" {
				marshalled_results[key] = append(marshalled_results[key],
					ResultJSON{"Distinct": len(v), "Count": len(v)})
			} else {
				for _, r := range v {
					_, ok := is_top_result[r.GroupByKey]
					if ok {
						marshalled_results[key] = append(marshalled_results[key], toResultJSON(r, querySpec))
					}
				}

			}

		}

		PrintJSON(marshalled_results)
		return
	}

	top_results := make([]string, 0)
	for _, r := range querySpec.Sorted {
		top_results = append(top_results, r.GroupByKey)
	}

	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 1, 0, ' ', tabwriter.AlignRight)

	for _, time_bucket := range keys {
		results := querySpec.TimeResults[time_bucket]
		time_str := time.Unix(int64(time_bucket), 0).Format(OPTS.TIME_FORMAT)

		if *FLAGS.OP == "distinct" {
			fmt.Fprintln(w, time_str, "\t", len(results), "\t")
		} else {
			for _, r := range results {
				if len(r.Hists) == 0 {
					fmt.Fprintln(w, time_str, "\t", r.Count, "\t", r.GroupByKey, "\t")
				} else {
					for agg, hist := range r.Hists {
						avg_str := fmt.Sprintf("%.2f", hist.Mean())
						fmt.Fprintln(w, time_str, "\t", r.Count, "\t", r.GroupByKey, "\t", agg, "\t", avg_str, "\t")
					}
				}

			}
		}
	}

	w.Flush()

}

func getSparseBuckets(buckets map[string]int64) map[string]int64 {
	non_zero_buckets := make(map[string]int64)
	for k, v := range buckets {
		if v > 0 {
			non_zero_buckets[k] = v
		}
	}

	return non_zero_buckets
}

func toResultJSON(r *specs.Result, querySpec *specs.QuerySpec) ResultJSON {

	var res = make(ResultJSON)
	for _, agg := range querySpec.Aggregations {
		if *FLAGS.OP == "hist" {
			inner := make(ResultJSON)
			res[agg.Name] = inner
			h := r.Hists[agg.Name]
			if h != nil {
				inner["percentiles"] = r.Hists[agg.Name].GetPercentiles()
				inner["buckets"] = getSparseBuckets(r.Hists[agg.Name].GetBuckets())
				inner["stddev"] = r.Hists[agg.Name].StdDev()
				inner["samples"] = r.Hists[agg.Name].TotalCount()
			}
		}

		if *FLAGS.OP == "avg" {
			result, ok := r.Hists[agg.Name]
			if ok {
				res[agg.Name] = result.Mean()
			} else {
				res[agg.Name] = nil
			}
		}
	}

	var group_key = strings.Split(r.GroupByKey, GROUP_DELIMITER)
	for i, g := range querySpec.Groups {
		res[g.Name] = group_key[i]
	}

	res["Count"] = r.Count
	res["Samples"] = r.Samples

	return res

}

func printSortedResults(querySpec *specs.QuerySpec) {
	sorted := querySpec.Sorted
	if int(querySpec.Limit) < len(querySpec.Sorted) {
		sorted = querySpec.Sorted[:querySpec.Limit]
	}

	if *FLAGS.JSON {
		var results = make([]ResultJSON, 0)

		if *FLAGS.OP == "distinct" {
			results = append(results, ResultJSON{"Distinct": len(querySpec.Results)})

		} else {

			for _, r := range sorted {
				var res = toResultJSON(r, querySpec)
				results = append(results, res)
			}
		}

		PrintJSON(results)
		return
	}

	if *FLAGS.OP == "distinct" {
		fmt.Println("DISTINCT RESULTS", len(querySpec.Results))
	} else {
		if len(sorted) > 1 {
			printResult(querySpec, querySpec.Cumulative)
		}

		for _, v := range sorted {
			printResult(querySpec, v)
		}
	}
}

func printResult(querySpec *specs.QuerySpec, v *specs.Result) {
	group_key := strings.Replace(v.GroupByKey, GROUP_DELIMITER, ",", -1)
	group_key = strings.TrimRight(group_key, ",")

	fmt.Printf(fmt.Sprintf("%-20s", group_key)[:20])

	fmt.Printf("%.0d", v.Count)
	if OPTS.WEIGHT_COL {
		fmt.Print(" (")
		fmt.Print(v.Samples)
		fmt.Print(")")
	}
	fmt.Printf("\n")

	for _, agg := range querySpec.Aggregations {
		col_name := fmt.Sprintf("  %5s", agg.Name)
		if *FLAGS.OP == "hist" {
			h, ok := v.Hists[agg.Name]
			if !ok {
				Debug("NO HIST AROUND FOR KEY", agg.Name, v.GroupByKey)
				continue
			}
			p := h.GetPercentiles()

			if len(p) > 0 {
				avg_str := fmt.Sprintf("%.2f", h.Mean())
				std_str := fmt.Sprintf("%.2f", h.StdDev())
				fmt.Println(col_name, "|", p[0], p[99], "|", avg_str, "|", p[0], p[25], p[50], p[75], p[99], "|", std_str)
			} else {
				fmt.Println(col_name, "No Data")
			}
		} else if *FLAGS.OP == "avg" {
			fmt.Println(col_name, fmt.Sprintf("%.2f", v.Hists[agg.Name].Mean()))
		}
	}
}

type ResultJSON map[string]interface{}

func PrintResults(querySpec *specs.QuerySpec) {
	if querySpec.TimeBucket > 0 {
		printTimeResults(querySpec)

		return
	}

	if *FLAGS.JSON {
		// Need to marshall
		var results = make([]ResultJSON, 0)

		for _, r := range querySpec.Results {
			var res = toResultJSON(r, querySpec)
			results = append(results, res)
		}

		PrintJSON(results)
		return
	}

	if FLAGS.OP != nil && *FLAGS.OP == "distinct" {
		fmt.Println("DISTINCT VALUES:", len(querySpec.Results))
	} else {
		count := 0

		Debug("PRINTING CUMULATIVE RESULT")
		if len(querySpec.Results) > 1 {
			printResult(querySpec, querySpec.Cumulative)
		}

		for _, v := range querySpec.Results {
			printResult(querySpec, v)
			count++
			if count >= int(querySpec.Limit) {
				return
			}
		}
	}
}

func PrintFinalResults(qs *specs.QuerySpec) {
	if *FLAGS.PRINT {
		if qs.TimeBucket > 0 {
			printTimeResults(qs)
		} else if qs.OrderBy != "" {
			printSortedResults(qs)
		} else {
			PrintResults(qs)
		}
	}
}

type Sample map[string]interface{}

func toTSVRow(r *Record) []string {

	row := make([]string, 0)
	for name, val := range r.Ints {
		if r.Populated[name] == INT_VAL {
			row = append(row, strconv.FormatInt(int64(val), 10))
		}
	}
	for name, val := range r.Strs {
		if r.Populated[name] == STR_VAL {
			col := md.GetColumnInfo(r.Block, int16(name))
			row = append(row, md.GetColumnStringForVal(col, int32(val)))
		}
	}

	return row

}

func SampleHeader(r *Record) []string {
	if r == nil {
		return nil
	}

	header := make([]string, 0)
	for name := range r.Ints {
		if r.Populated[name] == INT_VAL {
			header = append(header, md.GetBlockStringForKey(r.Block, int16(name)))
		}
	}
	for name := range r.Strs {
		if r.Populated[name] == STR_VAL {
			header = append(header, md.GetBlockStringForKey(r.Block, int16(name)))
		}
	}

	return header
}

func toSample(r *Record) *Sample {
	if r == nil {
		return nil
	}

	sample := Sample{}
	for name, val := range r.Ints {
		if r.Populated[name] == INT_VAL {
			sample[md.GetBlockStringForKey(r.Block, int16(name))] = val

		}
	}
	for name, val := range r.Strs {
		if r.Populated[name] == STR_VAL {
			col := md.GetColumnInfo(r.Block, int16(name))
			sample[md.GetBlockStringForKey(r.Block, int16(name))] = md.GetColumnStringForVal(col, int32(val))
		}
	}

	return &sample
}

func PrintSamples(t *Table) {
	count := 0
	records := make(RecordList, *FLAGS.LIMIT)
	for _, b := range t.BlockList {
		for _, r := range b.Matched {
			if r == nil {
				records = records[:count]
				break
			}

			if count >= *FLAGS.LIMIT {
				break
			}

			records[count] = r
			count++
		}

		if count >= *FLAGS.LIMIT {
			break
		}
	}

	if *FLAGS.JSON {
		samples := make([]*Sample, 0)
		for _, r := range records {
			if r == nil {
				break
			}

			s := toSample(r)
			samples = append(samples, s)
		}

		PrintJSON(samples)
	} else {
		for _, r := range records {
			if r == nil {
				break
			}

			PrintRecord(r)
		}
	}
	return
}

func PrintTables() {
	files, err := ioutil.ReadDir(*FLAGS.DIR)
	if err != nil {
		Error("No tables found!")
		return
	}

	tables := make([]string, 0)
	for _, db := range files {
		t := GetTable(db.Name())
		tables = append(tables, t.Name)
	}

	if *FLAGS.JSON {
		b, err := json.Marshal(tables)
		if err == nil {
			os.Stdout.Write(b)
		} else {
			Error("JSON encoding error", err)
		}

		return
	}

	for _, name := range tables {
		fmt.Print(name, " ")
	}

	fmt.Println("")

}

func getColsOfType(t *Table, wanted_type int8) []string {
	print_keys := make([]string, 0)
	for name, NameID := range t.KeyTable {
		col_type := t.KeyTypes[NameID]
		if int8(col_type) != wanted_type {
			continue
		}

		print_keys = append(print_keys, name)

	}
	sort.Strings(print_keys)

	return print_keys
}
func printColsOfType(t *Table, wanted_type int8) {
	for _, v := range getColsOfType(t, wanted_type) {
		fmt.Println(" ", v)
	}
}

func PrintColInfo(t *Table) {
	// count: 3253,
	// size: 908848,
	// avgObjSize: 279.3876421764525,
	// storageSize: 1740800,

	count := 0
	size := int64(0)
	for _, block := range t.BlockList {
		count += int(block.Info.NumRecords)
		size += block.Size
	}

	suffixes := []string{"B", "KB", "MB", "GB", "TB", "PB"}

	suffix_idx := 0

	small_size := size

	for ; small_size > 1024; small_size /= 1024 {
		suffix_idx += 1

	}

	if *FLAGS.JSON {
		table_cols := make(map[string][]string)
		metadata_io := make(map[string]interface{})

		table_cols["ints"] = getColsOfType(t, INT_VAL)
		table_cols["strs"] = getColsOfType(t, STR_VAL)
		table_cols["sets"] = getColsOfType(t, SET_VAL)
		metadata_io["columns"] = table_cols

		metadata_io["count"] = count
		metadata_io["size"] = size
		if count == 0 {
			count = 1
		}
		metadata_io["avgObjSize"] = float64(size) / float64(count)
		metadata_io["storageSize"] = size

		PrintJSON(metadata_io)
	} else {
		fmt.Println("\nString Columns\n")
		printColsOfType(t, STR_VAL)
		fmt.Println("\nInteger Columns\n")
		printColsOfType(t, INT_VAL)
		fmt.Println("\nSet Columns\n")
		printColsOfType(t, SET_VAL)
		fmt.Println("")
		fmt.Println("Stats")
		fmt.Println("  count", count)
		fmt.Println("  storageSize", small_size, suffixes[suffix_idx])
		fmt.Println("  avgObjSize", fmt.Sprintf("%.02f", float64(size)/float64(count)), "bytes")
	}

}

func PrintVersionInfo() {

	version_info := GetVersionInfo()

	if *FLAGS.JSON {
		PrintJSON(version_info)

	} else {
		for k, v := range version_info {
			fmt.Println(k, ":", v)
		}

	}

	return

}
func ExportBlockData(tb *TableBlock) {
	if len(tb.RecordList) == 0 {
		return
	}

	tsv_data := make([]string, 0)

	for _, r := range tb.RecordList {
		sample := toTSVRow(r)
		tsv_data = append(tsv_data, strings.Join(sample, "\t"))

	}

	export_name := path.Base(tb.Name)
	dir_name := path.Dir(tb.Name)
	fName := path.Join(dir_name, "export", export_name+".tsv.gz")

	os.MkdirAll(path.Join(dir_name, "export"), 0755)

	tsv_header := strings.Join(SampleHeader(tb.RecordList[0]), "\t")
	tsv_str := strings.Join(tsv_data, "\n")
	Debug("SAVING TSV ", len(tsv_str), "RECORDS", len(tsv_data), fName)

	all_data := strings.Join([]string{tsv_header, tsv_str}, "\n")
	// Need to save these to a file.
	//	Print(tsv_headers)
	//	Print(tsv_str)

	// GZIPPING
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Write([]byte(all_data))
	w.Close() // You must close this first to flush the bytes to the buffer.

	f, _ := os.Create(fName)
	_, err := f.Write(buf.Bytes())
	f.Close()

	if err != nil {
		Warn("COULDNT SAVE TSV FOR", fName, err)
	}

}

func PrintRecord(r *Record) {
	Print("RECORD", r)

	for name, val := range r.Ints {
		if r.Populated[name] == INT_VAL {
			Print("  ", name, md.GetBlockStringForKey(r.Block, int16(name)), val)
		}
	}
	for name, val := range r.Strs {
		if r.Populated[name] == STR_VAL {
			col := md.GetColumnInfo(r.Block, int16(name))
			Print("  ", name, md.GetBlockStringForKey(r.Block, int16(name)), md.GetColumnStringForVal(col, int32(val)))
		}
	}
	for name, vals := range r.SetMap {
		if r.Populated[name] == SET_VAL {
			col := md.GetColumnInfo(r.Block, int16(name))
			for _, val := range vals {
				Print("  ", name, md.GetBlockStringForKey(r.Block, int16(name)), val, md.GetColumnStringForVal(col, int32(val)))

			}

		}
	}
}

func PrintRecords(t *Table, records RecordList) {
	for i := 0; i < len(records); i++ {
		PrintRecord(records[i])
	}
}
