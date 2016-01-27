package edb
import "fmt"
import "flag"
import "strings"
import "time"

var f_RESET = flag.Bool("reset", false, "Reset the DB")
var f_TABLE = flag.String("table", "", "Table to operate on")
var f_ADD_RECORDS = flag.Int("add", 0, "Add data?")
var f_PRINT = flag.Bool("print", false, "Print some records")
var f_PRINT_INFO = flag.Bool("info", false, "Print table info")

var f_SESSION_COL = flag.String("session", "", "Column to use for sessionizing")
var f_INTS = flag.String("int", "", "Integer values to aggregate")
var f_STRS = flag.String("str", "", "String values to load")
var f_GROUPS = flag.String("group", "", "values group by")

var GROUP_BY  []string

func testTable(name string, loadSpec LoadSpec, querySpec QuerySpec) {
  table := getTable(name)

  lstart := time.Now()
  table.LoadRecords(&loadSpec)
  lend := time.Now()
  fmt.Println("LOADING RECORDS INTO TABLE TOOK", lend.Sub(lstart))

  filters := []Filter{}

  // TODO: ADD FILTER SPECIFICATIONS
  start := time.Now()
  ret := table.MatchRecords(filters)
  end := time.Now()
  fmt.Println("NO FILTER RETURNED", len(ret), "RECORDS, TOOK", end.Sub(start))

  age_filter := table.IntFilter("age", "lt", 20)
  filters = append(filters, age_filter)

  start = time.Now()
  filt_ret := table.MatchRecords(filters)
  end = time.Now()
  fmt.Println("INT FILTER RETURNED", len(filt_ret), "RECORDS, TOOK", end.Sub(start))

  table.AggRecords(ret, querySpec)
  table.AggRecords(filt_ret, querySpec)


  if (*f_SESSION_COL != "") {
    start = time.Now()
    session_maps := SessionizeRecords(ret, *f_SESSION_COL)
    end = time.Now()
    fmt.Println("SESSIONIZED", len(ret), "RECORDS INTO", len(session_maps), "SESSIONS, TOOK", end.Sub(start))

    start = time.Now()
    session_maps = SessionizeRecords(filt_ret, *f_SESSION_COL)
    end = time.Now()
    fmt.Println("SESSIONIZED", len(filt_ret), "RECORDS INTO", len(session_maps), "SESSIONS, TOOK", end.Sub(start))
  }
}

func ParseCmdLine() {
  flag.Parse()

  fmt.Println("Starting DB")
  fmt.Println("TABLE", *f_TABLE);


  add_records()

  table := *f_TABLE
  if table == "" { table = "test0" }

  ints := make([]string, 0)
  groups := make([]string, 0)
  strs := make([]string, 0)

  if *f_GROUPS != "" {
    groups = strings.Split(*f_GROUPS, ",")
    GROUP_BY = groups

  }

  if *f_STRS != "" {
    strs = strings.Split(*f_STRS, ",")

  }

  if *f_INTS != "" {
    ints = strings.Split(*f_INTS, ",")

  }



  groupings := []Grouping{}
  for _, g := range groups {
    groupings = append(groupings, Grouping{g})
  }

  aggs := []Aggregation {}
  for _, agg := range ints {
    aggs = append(aggs, Aggregation{op: "age", name: agg})
  }
  filters := []Filter{}

  querySpec := QuerySpec{Groups: groupings, Filters: filters, Aggregations: aggs }
  punctuateSpec(&querySpec)

  loadSpec := NewLoadSpec()
  for _, v := range groups { loadSpec.Str(v) }
  for _, v := range strs { loadSpec.Str(v) } 
  for _, v := range ints { loadSpec.Int(v) }


  fmt.Println("USING LOAD SPEC", loadSpec)

  fmt.Println("USING QUERY SPEC", querySpec)

  start := time.Now()
  testTable(table, loadSpec, querySpec)
  end := time.Now()
  fmt.Println("TESTING TABLE TOOK", end.Sub(start))

  start = time.Now()
  SaveTables()
  end = time.Now()
  fmt.Println("SERIALIZED DB TOOK", end.Sub(start))

  if *f_PRINT {
    t := getTable(table)
    count := 0
    for _, b := range t.BlockList {
      for _, r := range b.RecordList {
	count++
	t.PrintRecord(r)
	if count > 10 {
	  break
	}
      }

      if count > 10 {
	break
      }

    }

  }

  if *f_PRINT_INFO {
    t := getTable(table)
    t.PrintColInfo()
  }
}
