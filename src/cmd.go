package edb

import "fmt"
import "flag"
import "strings"
import "time"
import "strconv"
import "sync"


// TODO: add flag to shake the DB up and reload / resave data
var f_RESET = flag.Bool("reset", false, "Reset the DB")
var f_TABLE = flag.String("table", "", "Table to operate on")
var f_OP = flag.String("op", "avg", "metric to calculate, either 'avg' or 'hist'")
var f_ADD_RECORDS = flag.Int("add", 0, "Add data?")
var f_PRINT = flag.Bool("print", false, "Print some records")
var f_PRINT_INFO = flag.Bool("info", false, "Print table info")
var f_INT_FILTERS = flag.String("int-filter", "", "Int filters, format: col:op:val")
var f_STR_FILTERS = flag.String("str-filter", "", "Str filters, format: col:op:val")

var f_SESSION_COL = flag.String("session", "", "Column to use for sessionizing")
var f_INTS = flag.String("int", "", "Integer values to aggregate")
var f_STRS = flag.String("str", "", "String values to load")
var f_GROUPS = flag.String("group", "", "values group by")

var GROUP_BY  []string

func make_records(name string) {
  fmt.Println("Adding", *f_ADD_RECORDS, "to", name)
  CHUNK_SIZE := 50000
  var wg sync.WaitGroup
  for i := 0; i < *f_ADD_RECORDS  / CHUNK_SIZE; i++ {
    wg.Add(1)
    go func() {
      defer wg.Done()
      for j := 0; j < CHUNK_SIZE; j++ {
	NewRandomRecord(name); 
      }
    }()
  }

  for j := 0; j < *f_ADD_RECORDS % CHUNK_SIZE; j++ {
    NewRandomRecord(name); 
  }

  wg.Wait()


}

func add_records() {
  if (*f_ADD_RECORDS == 0) {
    return
  }


  fmt.Println("MAKING RECORDS FOR TABLE", *f_TABLE)
  if *f_TABLE != "" {
    make_records(*f_TABLE)
    return
  }

  var wg sync.WaitGroup
  for j := 0; j < 10; j++ {
    wg.Add(1)
    q := j
    go func() {
      defer wg.Done()
      table_name := fmt.Sprintf("test%v", q)
      make_records(table_name)
    }()
  }

  wg.Wait()


}
func queryTable(name string, loadSpec LoadSpec, querySpec QuerySpec) {
  table := getTable(name)

  table.MatchAndAggregate(querySpec)

  if *f_PRINT {
    for k, v := range querySpec.Results {

      fmt.Println(fmt.Sprintf("%-10s", k)[:10], fmt.Sprintf("%.0f", v.Ints["c"]))
      for _, agg := range querySpec.Aggregations {
	col_name := fmt.Sprintf("  %5s", agg.name)
	if *f_OP == "hist" {
	  h, ok := v.Hists[agg.name]
	  if !ok {
	    fmt.Println("NO HIST AROUND FOR KEY", agg.name, k)
	    continue
	  }
	  p := h.getPercentiles()
	  fmt.Println(col_name, p[0], p[25], p[50], p[75], p[99])
	} else if *f_OP == "avg" {
	  fmt.Println(col_name, fmt.Sprintf("%.2f", v.Ints[agg.name]))
	}
      }
    }
  }

  if (*f_SESSION_COL != "") {
    start := time.Now()
    session_maps := SessionizeRecords(querySpec.Matched, *f_SESSION_COL)
    end := time.Now()
    fmt.Println("SESSIONIZED", len(querySpec.Matched), "RECORDS INTO", len(session_maps), "SESSIONS, TOOK", end.Sub(start))
  }
}

func ParseCmdLine() {
  flag.Parse()

  fmt.Println("Starting DB")
  fmt.Println("TABLE", *f_TABLE);

  table := *f_TABLE
  if table == "" { table = "test0" }
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



  groupings := []Grouping{}
  for _, g := range groups {
    groupings = append(groupings, Grouping{g})
  }

  aggs := []Aggregation {}
  for _, agg := range ints {
    aggs = append(aggs, Aggregation{op: *f_OP, name: agg})
  }

  // LOAD TABLE INFOS BEFORE WE CREATE OUR FILTERS, SO WE CAN CREATE FILTERS ON
  // THE RIGHT COLUMN ID
  t.LoadRecords(nil)

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

  if *f_SESSION_COL != "" {
    loadSpec.Str(*f_SESSION_COL)
  }


  // add records should happen after we load records
  if (*f_ADD_RECORDS != 0) {	
    add_records()
  } else if !*f_PRINT_INFO {


    fmt.Println("USING LOAD SPEC", loadSpec)

    fmt.Println("USING QUERY SPEC", querySpec)


    start := time.Now()
    t.LoadRecords(&loadSpec)
    queryTable(table, loadSpec, querySpec)
    end := time.Now()
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
