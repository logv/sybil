package sybil

import "bytes"
import "encoding/gob"
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
	sorted := querySpec.Sorted
	if len(sorted) > int(querySpec.Limit) {
		sorted = sorted[:querySpec.Limit]
	}

	for _, result := range sorted {
		is_top_result[result.GroupByKey] = true
	}

	keys := make([]int, 0)

	for k, _ := range querySpec.TimeResults {
		keys = append(keys, k)
	}

	sort.Ints(keys)

	Debug("RESULT COUNT", len(keys))
	if FLAGS.JSON {

		marshalled_results := make(map[string][]ResultJSON)
		for k, v := range querySpec.TimeResults {
			key := strconv.FormatInt(int64(k), 10)
			marshalled_results[key] = make([]ResultJSON, 0)

			for _, r := range v {
				_, ok := is_top_result[r.GroupByKey]
				if ok {
					marshalled_results[key] = append(marshalled_results[key], r.toResultJSON(querySpec))
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

		time_str := time.Unix(int64(time_bucket), 0).Format(OPTS.TIME_FORMAT)
		results := querySpec.TimeResults[time_bucket]
		for _, r := range results {
			if len(querySpec.Distincts) > 0 {
				fmt.Fprintln(w, time_str, "\t", r.Distinct.Cardinality(), "\t", r.GroupByKey, "\t")

			} else if len(r.Hists) == 0 {
				fmt.Fprintln(w, time_str, "\t", r.Count, "\t", r.GroupByKey, "\t")
			} else {
				for agg, hist := range r.Hists {
					avg_str := fmt.Sprintf("%.2f", hist.Mean())
					fmt.Fprintln(w, time_str, "\t", r.Count, "\t", r.GroupByKey, "\t", agg, "\t", avg_str, "\t")
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
		if FLAGS.OP == "hist" {
			inner := make(ResultJSON)
			res[agg.Name] = inner
			h := r.Hists[agg.Name]
			if h != nil {
				inner["percentiles"] = r.Hists[agg.Name].GetPercentiles()
				inner["buckets"] = getSparseBuckets(r.Hists[agg.Name].GetStrBuckets())
				inner["stddev"] = r.Hists[agg.Name].StdDev()
				inner["avg"] = r.Hists[agg.Name].Mean()
				inner["sum"] = r.Hists[agg.Name].Mean() * float64(r.Hists[agg.Name].TotalCount())
				inner["samples"] = r.Hists[agg.Name].TotalCount()
			}
		}

		if FLAGS.OP == "avg" {
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

	if len(querySpec.Distincts) > 0 {
		res["Distinct"] = r.Distinct.Cardinality()
		res["Count"] = r.Distinct.Cardinality()
	} else {
		res["Count"] = r.Count
		res["Samples"] = r.Samples
	}

	return res

}

func printSortedResults(querySpec *QuerySpec) {
	sorted := querySpec.Sorted
	if int(querySpec.Limit) < len(querySpec.Sorted) {
		sorted = querySpec.Sorted[:querySpec.Limit]
	}

	if FLAGS.JSON {
		var results = make([]ResultJSON, 0)

		for _, r := range sorted {
			var res = r.toResultJSON(querySpec)
			results = append(results, res)
		}

		printJson(results)
		return
	}

	if querySpec.Cumulative != nil {
		percent_scanned := float64(querySpec.Cumulative.Count) / float64(querySpec.MatchedCount) * 100
		Debug("SCANNED", fmt.Sprintf("%.02f%%", percent_scanned), "(", querySpec.Cumulative.Count,
			") OF ROWS OUT OF", querySpec.MatchedCount)
	}

	if len(sorted) > 1 {
		printResult(querySpec, querySpec.Cumulative)
	}

	for _, v := range sorted {
		printResult(querySpec, v)
	}
}

func printResult(querySpec *QuerySpec, v *Result) {
	if v == nil {
		return
	}

	group_key := strings.Replace(v.GroupByKey, GROUP_DELIMITER, ",", -1)
	group_key = strings.TrimRight(group_key, ",")

	fmt.Printf(fmt.Sprintf("%-20s", group_key)[:20])

	fmt.Printf("%.0d", v.Count)
	if OPTS.WEIGHT_COL {
		fmt.Print(" (")
		fmt.Print(v.Samples)
		fmt.Print(")")
	}

	if len(querySpec.Distincts) > 0 {
		fmt.Print(" Distinct: ", v.Distinct.Cardinality())
	}

	fmt.Printf("\n")

	for _, agg := range querySpec.Aggregations {
		col_name := fmt.Sprintf("  %5s", agg.Name)
		if agg.Op == "hist" {
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
		} else if agg.Op == "avg" {
			fmt.Println(col_name, fmt.Sprintf("%.2f", v.Hists[agg.Name].Mean()))
		}
	}

}

type ResultJSON map[string]interface{}

func printResults(querySpec *QuerySpec) {
	if querySpec.TimeBucket > 0 {
		printTimeResults(querySpec)

		return
	}

	if FLAGS.JSON {
		// Need to marshall
		var results = make([]ResultJSON, 0)

		for _, r := range querySpec.Results {
			var res = r.toResultJSON(querySpec)
			results = append(results, res)
		}

		printJson(results)
		return
	}

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

func PrintBytes(obj interface{}) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(obj)
	if err != nil {
		Warn("COULDNT ENCODE BYTES", err)
	}

	Print(string(buf.Bytes()))

}

func encodeResults(qs *QuerySpec) {
	table := qs.Table
	qs.Table = nil
	PrintBytes(NodeResults{QuerySpec: *qs})
	qs.Table = table
}

func (qs *QuerySpec) PrintResults() {
	if FLAGS.ENCODE_RESULTS == true {
		Debug("ENCODING RESULTS")

		encodeResults(qs)
		return
	}

	if FLAGS.PRINT {
		if qs.TimeBucket > 0 {
			printTimeResults(qs)
		} else if qs.OrderBy != "" {
			printSortedResults(qs)
		} else {
			printResults(qs)
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

	for name, vals := range r.SetMap {
		if r.Populated[name] == SET_VAL {
			col := r.block.GetColumnInfo(int16(name))
			arr := make([]string, 0)

			for _, val := range vals {
				arr = append(arr, col.get_string_for_val(int32(val)))
			}
			sample[col.get_string_for_key(int(name))] = arr
		}
	}

	return &sample
}

type SortMatchedByCol struct {
	Matched []*Record

	Col string
}

func (a SortMatchedByCol) Len() int      { return len(a.Matched) }
func (a SortMatchedByCol) Swap(i, j int) { a.Matched[i], a.Matched[j] = a.Matched[j], a.Matched[i] }

// This sorts the records in descending order
func (a SortMatchedByCol) Less(i, j int) bool {
	if a.Col == SORT_COUNT {
		return i > j
	}

	t1, ok := a.Matched[i].getVal(a.Col)
	if !ok {
		return true
	}

	t2, ok := a.Matched[j].getVal(a.Col)
	if !ok {
		return false
	}

	return t1 > t2
}

func (t *Table) PrintSamples(qs *QuerySpec) {
	count := 0
	records := make(RecordList, 0)
	for _, b := range t.BlockList {
		for _, r := range b.Matched {
			if r == nil {
				break
			}

			records = append(records, r)
			count++
		}
	}

	reverse := false
	if qs != nil && qs.OrderBy != SORT_COUNT {
		sorter := SortMatchedByCol{}
		sorter.Matched = records
		sorter.Col = qs.OrderBy

		start := time.Now()
		sort.Sort(sorter)
		end := time.Now()
		if DEBUG_TIMING {
			Debug("SORTING MATCHES TOOK", end.Sub(start))
		}

		reverse = qs.OrderAsc
	} else { // backwards sort for samples
		reverse = true
	}

	if reverse {
		for i, j := 0, len(records)-1; i < j; i, j = i+1, j-1 {
			records[i], records[j] = records[j], records[i]
		}
	}

	if len(records) > FLAGS.LIMIT {
		records = records[:FLAGS.LIMIT]
	}

	samples := make([]*Sample, 0)
	for _, r := range records {
		if r == nil {
			break
		}

		s := r.toSample()
		samples = append(samples, s)
	}

	if FLAGS.ENCODE_RESULTS {
		Debug("NUMBER SAMPLES", len(samples))
		PrintBytes(NodeResults{Samples: samples})
		return
	}

	if FLAGS.JSON {
		printJson(samples)
		return
	}

	for _, r := range records {
		if r == nil {
			break
		}

		t.PrintRecord(r)
	}
}

func ListTables() []string {
	files, err := ioutil.ReadDir(FLAGS.DIR)
	if err != nil {
		Error("No tables found!")
		return []string{}
	}

	tables := make([]string, 0)
	for _, db := range files {
		t := GetTable(db.Name())
		tables = append(tables, t.Name)
	}

	return tables

}

func PrintTables() {
	tables := ListTables()

	printTablesToOutput(tables)

}

func printTablesToOutput(tables []string) {
	if FLAGS.ENCODE_RESULTS {
		PrintBytes(NodeResults{Tables: tables})
		return
	}

	if FLAGS.JSON {
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

	key_table := t.KeyTable
	key_types := t.KeyTypes

	// If we shortened the KeyTable during the query, we need to look into
	// AllKeyInfo for column information because KeyTable and KeyTypes were
	// truncated with only necessary columns
	if t.AllKeyInfo != nil {
		key_table = t.AllKeyInfo.KeyTable
		key_types = t.AllKeyInfo.KeyTypes
	}

	for name, name_id := range key_table {
		col_type := key_types[name_id]
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

type TableInfo struct {
	Count             int64   `json:"count"`
	Size              int64   `json:"storageSize"`
	AverageObjectSize float64 `json:"avgObjSize"`

	Columns ColumnInfo `json:"columns"`
}

type ColumnInfo struct {
	Strs []string `json:"strs"`
	Ints []string `json:"ints"`
	Sets []string `json:"sets"`
}

func (t *Table) TableInfo() *TableInfo {
	r := &TableInfo{}
	count := int64(0)
	size := int64(0)
	for _, block := range t.BlockList {
		count += int64(block.Info.NumRecords)
		size += block.Size
	}
	r.Count = count
	r.Size = size
	r.AverageObjectSize = float64(size) / float64(count)
	r.Columns.Strs = t.getColsOfType(STR_VAL)
	r.Columns.Ints = t.getColsOfType(INT_VAL)
	r.Columns.Sets = t.getColsOfType(SET_VAL)
	return r
}

func (t *Table) PrintTableInfo() {
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

	if FLAGS.ENCODE_RESULTS {
		PrintBytes(NodeResults{Table: *t})
		return
	}

	if FLAGS.JSON {
		table_info := t.TableInfo()
		printJson(table_info)
		return
	}

	fmt.Printf("\nString Columns\n")
	t.printColsOfType(STR_VAL)
	fmt.Printf("\nInteger Columns\n")
	t.printColsOfType(INT_VAL)
	fmt.Printf("\nSet Columns\n")
	t.printColsOfType(SET_VAL)
	fmt.Println("")
	fmt.Println("Stats")
	fmt.Println("  count", count)
	fmt.Println("  storageSize", small_size, suffixes[suffix_idx])
	fmt.Println("  avgObjSize", fmt.Sprintf("%.02f", float64(size)/float64(count)), "bytes")

}

func PrintVersionInfo() {

	version_info := GetVersionInfo()

	if FLAGS.JSON {
		printJson(version_info)

	} else {
		for k, v := range version_info {
			fmt.Println(k, ":", v)
		}

	}

	return

}
