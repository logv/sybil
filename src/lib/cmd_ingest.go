package edb

import (
    "encoding/json"
    "os"
    "flag"
    "io"
)

// appends records to our record input queue
// every now and then, we should pack the input queue into a column, though
func RunIngestCmdLine() {
     

    flag.Parse()

    if *f_TABLE == "" { flag.PrintDefaults(); return }

    t := getTable(*f_TABLE)
    t.LoadRecords(nil)

    dec := json.NewDecoder(os.Stdin)
    for {
        var recordmap map[string]interface{}

        if err := dec.Decode(&recordmap); err != nil {
	    if err == io.EOF {
	      break
	    }

            continue
        }

	r := t.NewRecord()

	intm := recordmap["ints"].(map[string]interface{})

	for k, v := range intm {
	  switch iv := v.(type) {
	    case float64: 
	      r.AddIntField(k, int(iv))

	  }
	}

	strm := recordmap["strs"].(map[string]interface{})
	for k, v := range strm {
	   switch iv := v.(type) {
	     case string:
	       r.AddStrField(k, iv)
	   }
	}

    }


    t.IngestRecords()
}

