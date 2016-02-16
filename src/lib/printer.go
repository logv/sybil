package sybil

import "log"
import "sort"
import "strings"
import "encoding/json"
import "strconv"
import "os"
import "fmt"
import "io/ioutil"

func printJson(data interface{}) {
	b, err := json.Marshal(data)
	if err == nil {
		os.Stdout.Write(b)
	} else {
		log.Fatal("JSON encoding error", err)
	}
}

func printTimeResults(querySpec *QuerySpec) {
	log.Println("PRINTING TIME RESULTS")
	log.Println("CHECKING SORT ORDER", len(querySpec.Sorted))

	is_top_result := make(map[string]bool)
	for _, result := range querySpec.Sorted {
		is_top_result[result.GroupByKey] = true
	}

	keys := make([]int, 0)

	for k, _ := range querySpec.TimeResults {
		keys = append(keys, k)
	}

	sort.Ints(keys)

	log.Println("RESULT COUNT", len(querySpec.TimeResults))
	if *f_JSON {

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

	for _, k := range keys {
		fmt.Println("BUCKET", k, len(querySpec.TimeResults[k]))
	}

}

func (r *Result) toResultJSON(querySpec *QuerySpec) ResultJSON {

	var res = make(ResultJSON)
	for _, agg := range querySpec.Aggregations {
		if *f_OP == "hist" {
			res[agg.name] = r.Hists[agg.name].getPercentiles()
		}

		if *f_OP == "avg" {
			res[agg.name] = r.Hists[agg.name].Avg
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

	if *f_JSON {
		var results = make([]ResultJSON, 0)

		for _, r := range sorted {
			var res = r.toResultJSON(querySpec)
			results = append(results, res)
		}

		printJson(results)
		return
	}

	for _, v := range sorted {
		printResult(querySpec, v)
	}
}

func printResult(querySpec *QuerySpec, v *Result) {
	group_key := strings.Replace(v.GroupByKey, GROUP_DELIMITER, ",", -1)
	group_key = strings.TrimRight(group_key, ",")

	fmt.Print(fmt.Sprintf("%-20s", group_key)[:20])

	fmt.Printf("%.0d", v.Count)
	if WEIGHT_COL {
		fmt.Print(" (")
		fmt.Print(v.Samples)
		fmt.Print(")")
	}
	fmt.Print("\n")

	for _, agg := range querySpec.Aggregations {
		col_name := fmt.Sprintf("  %5s", agg.name)
		if *f_OP == "hist" {
			h, ok := v.Hists[agg.name]
			if !ok {
				log.Println("NO HIST AROUND FOR KEY", agg.name, v.GroupByKey)
				continue
			}
			p := h.getPercentiles()

			if len(p) > 0 {
				avg_str := fmt.Sprintf("%.2f", h.Avg)
				fmt.Println(col_name, "|", h.Min, h.Max, "|", avg_str, "|", p[0], p[25], p[50], p[75], p[99])
			} else {
				fmt.Println(col_name, "No Data")
			}
		} else if *f_OP == "avg" {
			fmt.Println(col_name, fmt.Sprintf("%.2f", v.Hists[agg.name].Avg))
		}
	}
}

type ResultJSON map[string]interface{}

func printResults(querySpec *QuerySpec) {
	if querySpec.TimeBucket > 0 {
		printTimeResults(querySpec)

		return
	}

	if *f_JSON {
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
	for _, v := range querySpec.Results {
		printResult(querySpec, v)
		count++
		if count >= int(querySpec.Limit) {
			return
		}
	}
}

func (qs *QuerySpec) printResults() {
	if *f_PRINT {
		log.Println("PRINTING RESULTS")

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

func (t *Table) printSamples() {
	count := 0
	records := make(RecordList, *f_LIMIT)
	for _, b := range t.BlockList {
		for _, r := range b.Matched {
			if r == nil {
				records = records[:count]
				break
			}

			if count >= *f_LIMIT {
				break
			}

			records[count] = r
			count++
		}

		if count >= *f_LIMIT {
			break
		}
	}

	if *f_JSON {
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

func printTables() {
	files, err := ioutil.ReadDir(*f_DIR)
	if err != nil {
		log.Println("No tables found")
		return
	}

	tables := make([]string, 0)
	for _, db := range files {
		t := GetTable(db.Name())
		tables = append(tables, t.Name)
	}

	if *f_JSON {
		b, err := json.Marshal(tables)
		if err == nil {
			os.Stdout.Write(b)
		} else {
			log.Fatal("JSON encoding error", err)
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

	if *f_JSON {
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
		fmt.Println("  storageSize", size)
		fmt.Println("  avgObjSize", float64(size)/float64(count))
	}

}
