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

func printJSON(data interface{}) error {
	b, err := json.Marshal(data)
	if err == nil {
		os.Stdout.Write(b)
	} else {
		return err
	}
	return nil
}

func printTimeResults(printSpec *PrintSpec, querySpec *QuerySpec) error {
	Debug("PRINTING TIME RESULTS")
	Debug("CHECKING SORT ORDER", len(querySpec.Sorted))

	isTopResult := make(map[string]bool)
	sorted := querySpec.Sorted
	if len(sorted) > int(querySpec.Limit) {
		sorted = sorted[:querySpec.Limit]
	}

	for _, result := range sorted {
		isTopResult[result.GroupByKey] = true
	}

	keys := make([]int, 0)

	for k := range querySpec.TimeResults {
		keys = append(keys, k)
	}

	sort.Ints(keys)

	Debug("RESULT COUNT", len(keys))
	if printSpec.JSON {

		marshalledResults := make(map[string][]ResultJSON)
		for k, v := range querySpec.TimeResults {
			key := strconv.FormatInt(int64(k), 10)
			marshalledResults[key] = make([]ResultJSON, 0)

			for _, r := range v {
				_, ok := isTopResult[r.GroupByKey]
				if ok {
					marshalledResults[key] = append(marshalledResults[key], r.toResultJSON(printSpec, querySpec))
				}
			}
		}

		return printJSON(marshalledResults)
	}

	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 1, 0, ' ', tabwriter.AlignRight)

	for _, timeBucket := range keys {

		timeStr := time.Unix(int64(timeBucket), 0).String()
		results := querySpec.TimeResults[timeBucket]
		for _, r := range results {
			if len(querySpec.Distincts) > 0 {
				fmt.Fprintln(w, timeStr, "\t", r.Distinct.Cardinality(), "\t", r.GroupByKey, "\t")

			} else if len(r.Hists) == 0 {
				fmt.Fprintln(w, timeStr, "\t", r.Count, "\t", r.GroupByKey, "\t")
			} else {
				for agg, hist := range r.Hists {
					avgStr := fmt.Sprintf("%.2f", hist.Mean())
					fmt.Fprintln(w, timeStr, "\t", r.Count, "\t", r.GroupByKey, "\t", agg, "\t", avgStr, "\t")
				}
			}

		}
	}

	return w.Flush()
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

func (r *Result) toResultJSON(printSpec *PrintSpec, querySpec *QuerySpec) ResultJSON {

	var res = make(ResultJSON)
	for _, agg := range querySpec.Aggregations {
		if printSpec.Op == "hist" {
			inner := make(ResultJSON)
			res[agg.Name] = inner
			h := r.Hists[agg.Name]
			if h != nil {
				inner["percentiles"] = r.Hists[agg.Name].GetPercentiles()
				inner["buckets"] = getSparseBuckets(r.Hists[agg.Name].GetStrBuckets())
				inner["stddev"] = r.Hists[agg.Name].StdDev()
				inner["samples"] = r.Hists[agg.Name].TotalCount()
				inner["avg"] = r.Hists[agg.Name].Mean()
			}
		}

		if printSpec.Op == "avg" {
			result, ok := r.Hists[agg.Name]
			if ok {
				res[agg.Name] = result.Mean()
			} else {
				res[agg.Name] = nil
			}
		}
	}

	var groupKey = strings.Split(r.GroupByKey, GROUP_DELIMITER)
	for i, g := range querySpec.Groups {
		res[g.Name] = groupKey[i]
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

func printSortedResults(printSpec *PrintSpec, querySpec *QuerySpec) {
	sorted := querySpec.Sorted
	if int(querySpec.Limit) < len(querySpec.Sorted) {
		sorted = querySpec.Sorted[:querySpec.Limit]
	}

	if printSpec.JSON {
		var results = make([]ResultJSON, 0)

		for _, r := range sorted {
			var res = r.toResultJSON(printSpec, querySpec)
			results = append(results, res)
		}

		printJSON(results)
		return
	}

	if querySpec.Cumulative != nil {
		percentScanned := float64(querySpec.Cumulative.Count) / float64(querySpec.MatchedCount) * 100
		Debug("SCANNED", fmt.Sprintf("%.02f%%", percentScanned), "(", querySpec.Cumulative.Count,
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

	groupKey := strings.Replace(v.GroupByKey, GROUP_DELIMITER, ",", -1)
	groupKey = strings.TrimRight(groupKey, ",")

	fmt.Printf(fmt.Sprintf("%-20s", groupKey)[:20])

	fmt.Printf("%.0d", v.Count)
	if FLAGS.WEIGHT_COL != "" {
		fmt.Print(" (")
		fmt.Print(v.Samples)
		fmt.Print(")")
	}

	if len(querySpec.Distincts) > 0 {
		fmt.Print(" Distinct: ", v.Distinct.Cardinality())
	}

	fmt.Printf("\n")

	for _, agg := range querySpec.Aggregations {
		colName := fmt.Sprintf("  %5s", agg.Name)
		if agg.Op == OP_HIST {
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
		} else if agg.Op == OP_AVG {
			fmt.Println(colName, fmt.Sprintf("%.2f", v.Hists[agg.Name].Mean()))
		}
	}

}

type ResultJSON map[string]interface{}

func printResults(printSpec *PrintSpec, querySpec *QuerySpec) {
	if querySpec.TimeBucket > 0 {
		printTimeResults(printSpec, querySpec)

		return
	}

	if printSpec.JSON {
		// Need to marshall
		var results = make([]ResultJSON, 0)

		for _, r := range querySpec.Results {
			var res = r.toResultJSON(printSpec, querySpec)
			results = append(results, res)
		}

		printJSON(results)
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

func PrintBytes(obj interface{}) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(obj)
	if err != nil {
		return err
	}

	Print(buf.String())
	return nil
}

func encodeResults(qs *QuerySpec) {
	table := qs.Table
	qs.Table = nil
	PrintBytes(NodeResults{QuerySpec: *qs})
	qs.Table = table
}

func (qs *QuerySpec) PrintResults(printSpec *PrintSpec) {
	if printSpec.EncodeResults {
		Debug("ENCODING RESULTS")

		encodeResults(qs)
		return
	}

	if qs.TimeBucket > 0 {
		printTimeResults(printSpec, qs)
	} else if qs.OrderBy != "" {
		printSortedResults(printSpec, qs)
	} else {
		printResults(printSpec, qs)
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
	for name := range r.Ints {
		if r.Populated[name] == INT_VAL {
			col := r.block.GetColumnInfo(int16(name))
			header = append(header, col.getStringForKey(name))
		}
	}
	for name := range r.Strs {
		if r.Populated[name] == STR_VAL {
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
		if r.Populated[name] == INT_VAL {
			col := r.block.GetColumnInfo(int16(name))
			sample[col.getStringForKey(name)] = val

		}
	}
	for name, val := range r.Strs {
		if r.Populated[name] == STR_VAL {
			col := r.block.GetColumnInfo(int16(name))
			sample[col.getStringForKey(name)] = col.getStringForVal(int32(val))
		}
	}
	for name, vals := range r.SetMap {
		if r.Populated[name] == SET_VAL {
			col := r.block.GetColumnInfo(int16(name))
			sName := col.getStringForKey(int(name))
			sVals := []string{}
			for _, val := range vals {
				sVals = append(sVals, col.getStringForVal(int32(val)))
			}
			sample[sName] = sVals
		}
	}

	return &sample
}

type PrintSpec struct {
	ListTables bool
	PrintInfo  bool
	Samples    bool

	Op            Op
	Limit         int
	EncodeResults bool
	JSON          bool
}

func (t *Table) PrintSamples(printSpec *PrintSpec) {
	count := 0
	records := make(RecordList, printSpec.Limit)
	for _, b := range t.BlockList {
		for _, r := range b.Matched {
			if r == nil {
				records = records[:count]
				break
			}

			if count >= printSpec.Limit {
				break
			}

			records[count] = r
			count++
		}

		if count >= printSpec.Limit {
			break
		}
	}

	samples := make([]*Sample, 0)
	for _, r := range records {
		if r == nil {
			break
		}

		s := r.toSample()
		samples = append(samples, s)
	}

	if printSpec.EncodeResults {
		Debug("NUMBER SAMPLES", len(samples))
		PrintBytes(NodeResults{Samples: samples})
		return
	}

	if printSpec.JSON {

		printJSON(samples)
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
		return []string{}
	}

	tables := make([]string, 0)
	for _, db := range files {
		t := GetTable(db.Name())
		tables = append(tables, t.Name)
	}

	return tables

}

func PrintTables(printSpec *PrintSpec) {
	tables := ListTables()

	printTablesToOutput(printSpec, tables)

}

func printTablesToOutput(printSpec *PrintSpec, tables []string) error {
	if printSpec.EncodeResults {
		return PrintBytes(NodeResults{Tables: tables})
	}

	if printSpec.JSON {
		b, err := json.Marshal(tables)
		if err == nil {
			os.Stdout.Write(b)
		} else {
			return err
		}

		return nil
	}

	for _, name := range tables {
		fmt.Print(name, " ")
	}

	fmt.Println("")
	return nil
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

type ColInfo struct {
	Count             int64
	Size              int64
	AverageObjectSize float64
	Strs              []string
	Ints              []string
	Sets              []string
}

func (t *Table) ColInfo() *ColInfo {
	r := &ColInfo{}
	count := int64(0)
	size := int64(0)
	for _, block := range t.BlockList {
		count += int64(block.Info.NumRecords)
		size += block.Size
	}

	r.Count = count
	r.Size = size
	r.AverageObjectSize = float64(size) / float64(count)

	r.Strs = t.getColsOfType(STR_VAL)
	r.Ints = t.getColsOfType(INT_VAL)
	r.Sets = t.getColsOfType(SET_VAL)
	return r
}

func (t *Table) PrintColInfo(printSpec *PrintSpec) {
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

	suffixIdx := 0

	smallSize := size

	for ; smallSize > 1024; smallSize /= 1024 {
		suffixIdx++

	}

	if printSpec.EncodeResults {
		PrintBytes(NodeResults{Table: *t})
		return
	}

	if printSpec.JSON {
		tableCols := make(map[string][]string)
		tableInfo := make(map[string]interface{})

		tableCols["ints"] = t.getColsOfType(INT_VAL)
		tableCols["strs"] = t.getColsOfType(STR_VAL)
		tableCols["sets"] = t.getColsOfType(SET_VAL)
		tableInfo["columns"] = tableCols

		tableInfo["count"] = count
		tableInfo["size"] = size
		if count == 0 {
			count = 1
		}
		tableInfo["avgObjSize"] = float64(size) / float64(count)
		tableInfo["storageSize"] = size

		printJSON(tableInfo)
		return
	}

	fmt.Printf("\nString Columns\n\n")
	t.printColsOfType(STR_VAL)
	fmt.Printf("\nInteger Columns\n\n")
	t.printColsOfType(INT_VAL)
	fmt.Printf("\nSet Columns\n\n")
	t.printColsOfType(SET_VAL)
	fmt.Println("")
	fmt.Println("Stats")
	fmt.Println("  count", count)
	fmt.Println("  storageSize", smallSize, suffixes[suffixIdx])
	fmt.Println("  avgObjSize", fmt.Sprintf("%.02f", float64(size)/float64(count)), "bytes")

}

func PrintVersionInfo() {

	versionInfo := GetVersionInfo()

	if FLAGS.JSON {
		printJSON(versionInfo)

	} else {
		for k, v := range versionInfo {
			fmt.Println(k, ":", v)
		}

	}

}
