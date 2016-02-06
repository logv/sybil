package pcs

import "log"
import "sort"
import "encoding/json"
import "strconv"
import "os"
import "fmt"
import "io/ioutil"

func printTimeResults(querySpec *QuerySpec) {
	log.Println("PRINTING TIME RESULTS")

	keys := make([]int, 0)

	for k, _ := range querySpec.TimeResults {
		keys = append(keys, k)
	}

	sort.Sort(ByVal(keys))

	log.Println("RESULT COUNT", len(querySpec.TimeResults))
	if *f_JSON {

		marshalled_results := make(map[string]*ResultMap)
		for k, v := range querySpec.TimeResults {
			marshalled_results[strconv.FormatInt(int64(k), 10)] = &v
		}

		b, err := json.Marshal(marshalled_results)
		if err == nil {
			os.Stdout.Write(b)
		} else {
			log.Fatal("JSON encoding error", err)
		}

		return
	}

	for _, k := range keys {
		fmt.Println("BUCKET", k, len(querySpec.TimeResults[k]))
	}

}

func printSortedResults(querySpec *QuerySpec) {
	sorted := querySpec.Sorted
	if int(querySpec.Limit) < len(querySpec.Sorted) {
		sorted = querySpec.Sorted[:querySpec.Limit]
	}

	if *f_JSON {
		b, err := json.Marshal(sorted)
		if err == nil {
			os.Stdout.Write(b)
		} else {
			log.Fatal("JSON encoding error", err)
		}

		return
	}

	for _, v := range sorted {
		printResult(querySpec, v)
	}
}

func printResult(querySpec *QuerySpec, v *Result) {
	fmt.Println(fmt.Sprintf("%-20s", v.GroupByKey)[:20], fmt.Sprintf("%.0d", v.Count))
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
				fmt.Println(col_name, avg_str, p[0], p[25], p[50], p[75], p[99])
			} else {
				fmt.Println(col_name, "No Data")
			}
		} else if *f_OP == "avg" {
			fmt.Println(col_name, fmt.Sprintf("%.2f", v.Ints[agg.name]))
		}
	}
}

func printResults(querySpec *QuerySpec) {
	if querySpec.TimeBucket > 0 {
		printTimeResults(querySpec)

		return
	}

	if *f_JSON {
		b, err := json.Marshal(querySpec.Results)
		if err == nil {
			os.Stdout.Write(b)
		} else {
			log.Fatal("JSON encoding error", err)
		}

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
	sample := Sample{}
	for name, val := range r.Ints {
		if r.Populated[name] == INT_VAL {
			col := r.block.getColumnInfo(int16(name))
			sample[col.get_string_for_key(name)] = val

		}
	}
	for name, val := range r.Strs {
		if r.Populated[name] == STR_VAL {
			col := r.block.getColumnInfo(int16(name))
			sample[col.get_string_for_key(name)] = col.get_string_for_val(int32(val))
		}
	}

	return &sample
}

func (t *Table) printSamples() {
	count := 0
	records := make([]*Record, *f_LIMIT)
	for _, b := range t.BlockList {
		for _, r := range b.RecordList {
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
			s := r.toSample()
			samples = append(samples, s)
		}

		b, err := json.Marshal(samples)
		if err == nil {
			os.Stdout.Write(b)
		} else {
			log.Fatal("JSON encoding error", err)
		}

	} else {
		for _, r := range records {
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
