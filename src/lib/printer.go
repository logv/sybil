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

	isTopResult := make(map[string]bool)
	for _, result := range querySpec.Sorted {
		isTopResult[result.GroupByKey] = true
	}

	keys := make([]int, 0)

	for k, _ := range querySpec.TimeResults {
		keys = append(keys, k)
	}

	sort.Ints(keys)

	Debug("RESULT COUNT", len(querySpec.TimeResults))
	if *FLAGS.JSON {

		marshalledResults := make(map[string][]ResultJSON)
		for k, v := range querySpec.TimeResults {
			key := strconv.FormatInt(int64(k), 10)
			marshalledResults[key] = make([]ResultJSON, 0)

			if *FLAGS.Op == "distinct" {
				marshalledResults[key] = append(marshalledResults[key],
					ResultJSON{"Distinct": len(v), "Count": len(v)})
			} else {
				for _, r := range v {
					_, ok := isTopResult[r.GroupByKey]
					if ok {
						marshalledResults[key] = append(marshalledResults[key], r.toResultJSON(querySpec))
					}
				}

			}

		}

		printJson(marshalledResults)
		return
	}

	topResults := make([]string, 0)
	for _, r := range querySpec.Sorted {
		topResults = append(topResults, r.GroupByKey)
	}

	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 1, 0, ' ', tabwriter.AlignRight)

	for _, timeBucket := range keys {
		results := querySpec.TimeResults[timeBucket]
		timeStr := time.Unix(int64(timeBucket), 0).Format(OPTS.TimeFormat)

		if *FLAGS.Op == "distinct" {
			fmt.Fprintln(w, timeStr, "\t", len(results), "\t")
		} else {
			for _, r := range results {
				if len(r.Hists) == 0 {
					fmt.Fprintln(w, timeStr, "\t", r.Count, "\t", r.GroupByKey, "\t")
				} else {
					for agg, hist := range r.Hists {
						avgStr := fmt.Sprintf("%.2f", hist.Mean())
						fmt.Fprintln(w, timeStr, "\t", r.Count, "\t", r.GroupByKey, "\t", agg, "\t", avgStr, "\t")
					}
				}

			}
		}
	}

	w.Flush()

}

func getSparseBuckets(buckets map[string]int64) map[string]int64 {
	nonZeroBuckets := make(map[string]int64)
	for k, v := range buckets {
		if v > 0 {
			nonZeroBuckets[k] = v
		}
	}

	return nonZeroBuckets
}

func (r *Result) toResultJSON(querySpec *QuerySpec) ResultJSON {

	var res = make(ResultJSON)
	for _, agg := range querySpec.Aggregations {
		if *FLAGS.Op == "hist" {
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

		if *FLAGS.Op == "avg" {
			result, ok := r.Hists[agg.Name]
			if ok {
				res[agg.Name] = result.Mean()
			} else {
				res[agg.Name] = nil
			}
		}
	}

	var groupKey = strings.Split(r.GroupByKey, GroupDelimiter)
	for i, g := range querySpec.Groups {
		res[g.Name] = groupKey[i]
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

		if *FLAGS.Op == "distinct" {
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

	if *FLAGS.Op == "distinct" {
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
	groupKey := strings.Replace(v.GroupByKey, GroupDelimiter, ",", -1)
	groupKey = strings.TrimRight(groupKey, ",")

	fmt.Printf(fmt.Sprintf("%-20s", groupKey)[:20])

	fmt.Printf("%.0d", v.Count)
	if OPTS.WeightCol {
		fmt.Print(" (")
		fmt.Print(v.Samples)
		fmt.Print(")")
	}
	fmt.Printf("\n")

	for _, agg := range querySpec.Aggregations {
		colName := fmt.Sprintf("  %5s", agg.Name)
		if *FLAGS.Op == "hist" {
			h, ok := v.Hists[agg.Name]
			if !ok {
				Debug("NO HIST AROUND FOR KEY", agg.Name, v.GroupByKey)
				continue
			}
			p := h.GetPercentiles()

			if len(p) > 0 {
				avgStr := fmt.Sprintf("%.2f", h.Mean())
				stdStr := fmt.Sprintf("%.2f", h.StdDev())
				fmt.Println(colName, "|", p[0], p[99], "|", avgStr, "|", p[0], p[25], p[50], p[75], p[99], "|", stdStr)
			} else {
				fmt.Println(colName, "No Data")
			}
		} else if *FLAGS.Op == "avg" {
			fmt.Println(colName, fmt.Sprintf("%.2f", v.Hists[agg.Name].Mean()))
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

	if FLAGS.Op != nil && *FLAGS.Op == "distinct" {
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
	if *FLAGS.Print {
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
		if r.Populated[name] == IntVal {
			row = append(row, strconv.FormatInt(int64(val), 10))
		}
	}
	for name, val := range r.Strs {
		if r.Populated[name] == StrVal {
			col := r.block.GetColumnInfo(int16(name))
			row = append(row, col.getStringForVal(int32(val)))
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
		if r.Populated[name] == IntVal {
			col := r.block.GetColumnInfo(int16(name))
			header = append(header, col.getStringForKey(name))
		}
	}
	for name, _ := range r.Strs {
		if r.Populated[name] == StrVal {
			col := r.block.GetColumnInfo(int16(name))
			header = append(header, col.getStringForKey(name))
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
		if r.Populated[name] == IntVal {
			col := r.block.GetColumnInfo(int16(name))
			sample[col.getStringForKey(name)] = val

		}
	}
	for name, val := range r.Strs {
		if r.Populated[name] == StrVal {
			col := r.block.GetColumnInfo(int16(name))
			sample[col.getStringForKey(name)] = col.getStringForVal(int32(val))
		}
	}

	return &sample
}

func (t *Table) PrintSamples() {
	count := 0
	records := make(RecordList, *FLAGS.Limit)
	for _, b := range t.BlockList {
		for _, r := range b.Matched {
			if r == nil {
				records = records[:count]
				break
			}

			if count >= *FLAGS.Limit {
				break
			}

			records[count] = r
			count++
		}

		if count >= *FLAGS.Limit {
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
	files, err := ioutil.ReadDir(*FLAGS.Dir)
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

func (t *Table) getColsOfType(wantedType int8) []string {
	printKeys := make([]string, 0)
	for name, nameID := range t.KeyTable {
		colType := t.KeyTypes[nameID]
		if int8(colType) != wantedType {
			continue
		}

		printKeys = append(printKeys, name)

	}
	sort.Strings(printKeys)

	return printKeys
}
func (t *Table) printColsOfType(wantedType int8) {
	for _, v := range t.getColsOfType(wantedType) {
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

	suffixIDx := 0

	smallSize := size

	for ; smallSize > 1024; smallSize /= 1024 {
		suffixIDx += 1

	}

	if *FLAGS.JSON {
		tableCols := make(map[string][]string)
		tableInfo := make(map[string]interface{})

		tableCols["ints"] = t.getColsOfType(IntVal)
		tableCols["strs"] = t.getColsOfType(StrVal)
		tableCols["sets"] = t.getColsOfType(SetVal)
		tableInfo["columns"] = tableCols

		tableInfo["count"] = count
		tableInfo["size"] = size
		if count == 0 {
			count = 1
		}
		tableInfo["avgObjSize"] = float64(size) / float64(count)
		tableInfo["storageSize"] = size

		printJson(tableInfo)
	} else {
		fmt.Println("\nString Columns\n")
		t.printColsOfType(StrVal)
		fmt.Println("\nInteger Columns\n")
		t.printColsOfType(IntVal)
		fmt.Println("\nSet Columns\n")
		t.printColsOfType(SetVal)
		fmt.Println("")
		fmt.Println("Stats")
		fmt.Println("  count", count)
		fmt.Println("  storageSize", smallSize, suffixes[suffixIDx])
		fmt.Println("  avgObjSize", fmt.Sprintf("%.02f", float64(size)/float64(count)), "bytes")
	}

}

func PrintVersionInfo() {

	versionInfo := GetVersionInfo()

	if *FLAGS.JSON {
		printJson(versionInfo)

	} else {
		for k, v := range versionInfo {
			fmt.Println(k, ":", v)
		}

	}

	return

}
