package sybil

import "sort"
import "strings"
import "encoding/json"
import "strconv"
import "os"
import "fmt"
import "io/ioutil"
import "text/tabwriter"
import "time"

func printJson(data interface{}) {
	b, err := json.Marshal(data)
	if err == nil {
		os.Stdout.Write(b)
	} else {
		Error("JSON encoding error", err)
	}
}

func printTimeResults(querySpec *QuerySpec) {
	Debug("PRINTING TIME RESULTS")
	Debug("CHECKING SORT ORDER", len(querySpec.Sorted))

	is_top_result := make(map[string]bool)
	for _, result := range querySpec.Sorted {
		is_top_result[result.GroupByKey] = true
	}

	keys := make([]int, 0)

	for k, _ := range querySpec.TimeResults {
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
						marshalled_results[key] = append(marshalled_results[key], r.toResultJSON(querySpec))
					}
				}

			}

		}

		printJson(marshalled_results)
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

func (r *Result) toResultJSON(querySpec *QuerySpec) ResultJSON {

	var res = make(ResultJSON)
	for _, agg := range querySpec.Aggregations {
		if *FLAGS.OP == "hist" {
			inner := make(ResultJSON)
			res[agg.name] = inner
			h := r.Hists[agg.name]
			if h != nil {
				inner["percentiles"] = r.Hists[agg.name].GetPercentiles()
				inner["buckets"] = getSparseBuckets(r.Hists[agg.name].GetBuckets())
				inner["stddev"] = r.Hists[agg.name].StdDev()
				inner["samples"] = r.Hists[agg.name].TotalCount()
			}
		}

		if *FLAGS.OP == "avg" {
			result, ok := r.Hists[agg.name]
			if ok {
				res[agg.name] = result.Mean()
			} else {
				res[agg.name] = nil
			}
		}
	}

	var group_key = strings.Split(r.GroupByKey, GROUP_DELIMITER)
	for i, g := range querySpec.Groups {
		res[g.name] = group_key[i]
	}

	res["Count"] = r.Count
	res["Samples"] = r.Samples

	return res

}

func printSortedResults(querySpec *QuerySpec) {
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
				var res = r.toResultJSON(querySpec)
				results = append(results, res)
			}
		}

		printJson(results)
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

func printResult(querySpec *QuerySpec, v *Result) {
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
		col_name := fmt.Sprintf("  %5s", agg.name)
		if *FLAGS.OP == "hist" {
			h, ok := v.Hists[agg.name]
			if !ok {
				Debug("NO HIST AROUND FOR KEY", agg.name, v.GroupByKey)
				continue
			}
			p := h.GetPercentiles()

			if len(p) > 0 {
				avg_str := fmt.Sprintf("%.2f", h.Mean())
				std_str := fmt.Sprintf("%.2f", h.StdDev())
				fmt.Println(col_name, "|", h.Min(), h.Max(), "|", avg_str, "|", p[0], p[25], p[50], p[75], p[99], "|", std_str)
			} else {
				fmt.Println(col_name, "No Data")
			}
		} else if *FLAGS.OP == "avg" {
			fmt.Println(col_name, fmt.Sprintf("%.2f", v.Hists[agg.name].Mean()))
		}
	}
}

type ResultJSON map[string]interface{}

func PrintResults(querySpec *QuerySpec) {
	if querySpec.TimeBucket > 0 {
		printTimeResults(querySpec)

		return
	}

	if *FLAGS.JSON {
		// Need to marshall
		var results = make([]ResultJSON, 0)

		for _, r := range querySpec.Results {
			var res = r.toResultJSON(querySpec)
			results = append(results, res)
		}

		printJson(results)
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

func (qs *QuerySpec) PrintResults() {
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

func (r *Record) toTSVRow() []string {

	row := make([]string, 0)
	for name, val := range r.Ints {
		if r.Populated[name] == INT_VAL {
			row = append(row, strconv.FormatInt(int64(val), 10))
		}
	}
	for name, val := range r.Strs {
		if r.Populated[name] == STR_VAL {
			col := r.block.GetColumnInfo(int16(name))
			row = append(row, col.get_string_for_val(int32(val)))
		}
	}

	return row

}

func (r *Record) sampleHeader() []string {
	if r == nil {
		return nil
	}

	header := make([]string, 0)
	for name, _ := range r.Ints {
		if r.Populated[name] == INT_VAL {
			col := r.block.GetColumnInfo(int16(name))
			header = append(header, col.get_string_for_key(name))
		}
	}
	for name, _ := range r.Strs {
		if r.Populated[name] == STR_VAL {
			col := r.block.GetColumnInfo(int16(name))
			header = append(header, col.get_string_for_key(name))
		}
	}

	return header
}

func (r *Record) toSample() *Sample {
	if r == nil {
		return nil
	}

	sample := Sample{}
	for name, val := range r.Ints {
		if r.Populated[name] == INT_VAL {
			col := r.block.GetColumnInfo(int16(name))
			sample[col.get_string_for_key(name)] = val

		}
	}
	for name, val := range r.Strs {
		if r.Populated[name] == STR_VAL {
			col := r.block.GetColumnInfo(int16(name))
			sample[col.get_string_for_key(name)] = col.get_string_for_val(int32(val))
		}
	}

	return &sample
}

func (t *Table) PrintSamples() {
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

			s := r.toSample()
			samples = append(samples, s)
		}

		printJson(samples)
	} else {
		for _, r := range records {
			if r == nil {
				break
			}

			t.PrintRecord(r)
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

func (t *Table) getColsOfType(wanted_type int8) []string {
	print_keys := make([]string, 0)
	for name, name_id := range t.KeyTable {
		col_type := t.KeyTypes[name_id]
		if int8(col_type) != wanted_type {
			continue
		}

		print_keys = append(print_keys, name)

	}
	sort.Strings(print_keys)

	return print_keys
}
func (t *Table) printColsOfType(wanted_type int8) {
	for _, v := range t.getColsOfType(wanted_type) {
		fmt.Println(" ", v)
	}
}

func (t *Table) PrintColInfo() {
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
		table_info := make(map[string]interface{})

		table_cols["ints"] = t.getColsOfType(INT_VAL)
		table_cols["strs"] = t.getColsOfType(STR_VAL)
		table_cols["sets"] = t.getColsOfType(SET_VAL)
		table_info["columns"] = table_cols

		table_info["count"] = count
		table_info["size"] = size
		if count == 0 {
			count = 1
		}
		table_info["avgObjSize"] = float64(size) / float64(count)
		table_info["storageSize"] = size

		printJson(table_info)
	} else {
		fmt.Println("\nString Columns\n")
		t.printColsOfType(STR_VAL)
		fmt.Println("\nInteger Columns\n")
		t.printColsOfType(INT_VAL)
		fmt.Println("\nSet Columns\n")
		t.printColsOfType(SET_VAL)
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
		printJson(version_info)

	} else {
		for k, v := range version_info {
			fmt.Println(k, ":", v)
		}

	}

	return

}
