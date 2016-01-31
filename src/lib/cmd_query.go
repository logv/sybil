package edb

import "fmt"
import "flag"
import "strings"
import "time"
import "strconv"
import "runtime/debug"

var MAX_RECORDS_NO_GC = 4 * 1000 * 1000 // 4 million


func printResult(querySpec *QuerySpec, v *Result) {
  fmt.Println(fmt.Sprintf("%-10s", v.GroupByKey)[:10], fmt.Sprintf("%.0d", v.Count))
  for _, agg := range querySpec.Aggregations {
    col_name := fmt.Sprintf("  %5s", agg.name)
    if *f_OP == "hist" {
      h, ok := v.Hists[agg.name]
      if !ok {
	fmt.Println("NO HIST AROUND FOR KEY", agg.name, v.GroupByKey)
	continue
      }
      p := h.getPercentiles()

      if len(p) > 0 {
	fmt.Println(col_name, p[0], p[25], p[50], p[75], p[99])
      } else {
	fmt.Println(col_name, "No Data")
      }
    } else if *f_OP == "avg" {
      fmt.Println(col_name, fmt.Sprintf("%.2f", v.Ints[agg.name]))
    }
  }
}
func printResults(querySpec *QuerySpec) {
  count := 0
  for _, v := range querySpec.Results {
    printResult(querySpec, v)
    count ++
    if (count > int(querySpec.Limit)) {
      return
    }
  }
}

func printSortedResults(querySpec *QuerySpec) {
  for _, v := range querySpec.Sorted[querySpec.Limit:] {
    printResult(querySpec, v)
  }
}

func queryTable(name string, loadSpec *LoadSpec, querySpec *QuerySpec) {
  table := getTable(name)

  table.MatchAndAggregate(querySpec)

  if *f_PRINT {
    fmt.Println("PRINTING RESULTS")

    if querySpec.OrderBy != "" {
      printSortedResults(querySpec)
    } else {
      printResults(querySpec)
    }
  }

  if (*f_SESSION_COL != "") {
    start := time.Now()
    session_maps := SessionizeRecords(querySpec.Matched, *f_SESSION_COL)
    end := time.Now()
    fmt.Println("SESSIONIZED", len(querySpec.Matched), "RECORDS INTO", len(session_maps), "SESSIONS, TOOK", end.Sub(start))
  }
}

func addFlags() {

  f_OP = flag.String("op", "avg", "metric to calculate, either 'avg' or 'hist'")
  f_PRINT = flag.Bool("print", false, "Print some records")
  f_INT_FILTERS = flag.String("int-filter", "", "Int filters, format: col:op:val")
  f_STR_FILTERS = flag.String("str-filter", "", "Str filters, format: col:op:val")

  f_SESSION_COL = flag.String("session", "", "Column to use for sessionizing")
  f_INTS = flag.String("int", "", "Integer values to aggregate")
  f_STRS = flag.String("str", "", "String values to load")
  f_GROUPS = flag.String("group", "", "values group by")
}

func RunQueryCmdLine() {
  addFlags()
  flag.Parse()

  table := *f_TABLE
  if table == "" { flag.PrintDefaults(); return }
  t := getTable(table)

  ints := make([]string, 0)
  groups := make([]string, 0)
  strs := make([]string, 0)
  strfilters := make([]string, 0)
  intfilters := make([]string, 0)

  if *f_GROUPS != "" {
    groups = strings.Split(*f_GROUPS, ",")
    GROUP_BY = groups

  }

  // PROCESS CMD LINE ARGS THAT USE COMMA DELIMITERS
  if *f_STRS != "" { strs = strings.Split(*f_STRS, ",") }
  if *f_INTS != "" { ints = strings.Split(*f_INTS, ",") } 
  if *f_INT_FILTERS != "" { intfilters = strings.Split(*f_INT_FILTERS, ",") }
  if *f_STR_FILTERS != "" { strfilters = strings.Split(*f_STR_FILTERS, ",") }

  
  if *f_PROFILE && PROFILER_ENABLED {
    profile := RUN_PROFILER()
    defer profile.Start().Stop()
  }

  // LOAD TABLE INFOS BEFORE WE CREATE OUR FILTERS, SO WE CAN CREATE FILTERS ON
  // THE RIGHT COLUMN ID
  t.LoadRecords(nil)

  groupings := []Grouping{}
  for _, g := range groups {
    col_id := t.get_key_id(g)
    groupings = append(groupings, Grouping{g, col_id})
  }

  aggs := []Aggregation {}
  for _, agg := range ints {
    col_id := t.get_key_id(agg)
    aggs = append(aggs, Aggregation{op: *f_OP, name: agg, name_id: col_id})
  }


  // VERIFY THE KEY TABLE IS IN ORDER, OTHERWISE WE NEED TO EXIT
  fmt.Println("KEY TABLE", t.KeyTable)
  used := make(map[int16]int)
  for _, v := range t.KeyTable {
    used[v]++
    if used[v] > 1 {
      fmt.Println("THERE IS A SERIOUS KEY TABLE INCONSISTENCY")
      return
    }
  }


  loadSpec := NewLoadSpec()
  filters := []Filter{}
  for _, filt := range intfilters {
    tokens := strings.Split(filt, ":")
    col := tokens[0]
    op := tokens[1]
    val, _ := strconv.ParseInt(tokens[2], 10, 64)

    filters = append(filters, t.IntFilter(col, op, int(val)))
    loadSpec.Int(col)
  }

  for _, filter := range strfilters {
    tokens := strings.Split(filter, ":")
    col := tokens[0]
    op := tokens[1]
    val := tokens[2]
    loadSpec.Str(col)

    filters = append(filters, t.StrFilter(col, op, val))

  }

  querySpec := QuerySpec{Groups: groupings, Filters: filters, Aggregations: aggs }
  punctuateSpec(&querySpec)

  for _, v := range groups { loadSpec.Str(v) }
  for _, v := range strs { loadSpec.Str(v) } 
  for _, v := range ints { loadSpec.Int(v) }

  if *f_SORT != "" {
    loadSpec.Int(*f_SORT)
    querySpec.OrderBy = *f_SORT
  } else {
    querySpec.OrderBy = ""
  }

  querySpec.Limit = int16(*f_LIMIT)

  if *f_SESSION_COL != "" {
    loadSpec.Str(*f_SESSION_COL)
  }


  if !*f_PRINT_INFO {
    // DISABLE GC FOR QUERY PATH
    // NEVER TURN IT BACK ON!
    fmt.Println("ADDING BULLET HOLES FOR SPEED (DISABLING GC)")
    old_percent := debug.SetGCPercent(-1)
    


    fmt.Println("USING LOAD SPEC", loadSpec)

    fmt.Println("USING QUERY SPEC", querySpec)

  
    
    start := time.Now()
    count := t.LoadRecords(&loadSpec)
    end := time.Now()
    

    fmt.Println("LOAD RECORDS TOOK", end.Sub(start))
    if count > MAX_RECORDS_NO_GC { 
      fmt.Println("MORE THAN", fmt.Sprintf("%dm", MAX_RECORDS_NO_GC / 1000 / 1000), "RECORDS LOADED ENABLING GC")
      gc_start := time.Now()
      debug.SetGCPercent(old_percent)
      end = time.Now()
      fmt.Println("GC TOOK", end.Sub(gc_start))
    }

    queryTable(table, &loadSpec, &querySpec)
    end = time.Now()
    fmt.Println("LOADING & QUERYING TABLE TOOK", end.Sub(start))
  }

  start := time.Now()
  t.SaveRecords()
  end := time.Now()
  fmt.Println("SERIALIZED DB TOOK", end.Sub(start))

  if *f_PRINT_INFO {
    t := getTable(table)
    t.PrintColInfo()
  }
}
