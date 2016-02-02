package edb

import "flag"
import "fmt"
import "sync"
import "time"
import "runtime/debug"

// IS DEFINED IN QUERY_CMD WEIRDLY ENOUGH
//var f_TABLE = flag.String("table", "", "Table to operate on")

func make_records(name string) {
	fmt.Println("Adding", *f_ADD_RECORDS, "to", name)
	t := getTable(name)

	for j := 0; j < CHUNK_SIZE; j++ {
		NewRandomRecord(name)
	}

	t.FillPartialBlock()
	remainder := t.newRecords

	t.newRecords = make([]*Record, 0)
	for i := 1; i < *f_ADD_RECORDS/CHUNK_SIZE; i++ {
		for j := 0; j < CHUNK_SIZE; j++ {
			NewRandomRecord(name)
		}

		t.SaveRecords()
		t.ReleaseRecords()
	}

	for j := 0; j < *f_ADD_RECORDS%CHUNK_SIZE; j++ {
		NewRandomRecord(name)
	}

	t.newRecords = append(t.newRecords, remainder...)

}

func add_records() {
	if *f_ADD_RECORDS == 0 {
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

func RunWriteCmdLine() {
	f_ADD_RECORDS = flag.Int("add", 0, "Add data?")

	flag.Parse()
	if *f_PROFILE && PROFILER_ENABLED {
		profile := RUN_PROFILER()
		defer profile.Start().Stop()
	}

	if *f_TABLE == "" {
		flag.PrintDefaults()
		return
	}

	t := getTable(*f_TABLE)

	t.LoadRecords(nil)

	if *f_ADD_RECORDS != 0 {
		if *f_ADD_RECORDS < 500000 {
			fmt.Println("ADDING BULLET HOLES FOR SPEED (DISABLING GC)")
			debug.SetGCPercent(-1)
		}
		add_records()
	}

	start := time.Now()
	t.SaveRecords()
	end := time.Now()
	fmt.Println("SERIALIZED DB TOOK", end.Sub(start))

	if *f_PRINT_INFO {
		t.PrintColInfo()
	}
}
