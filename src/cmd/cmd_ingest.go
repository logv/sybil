package sybilCmd

import sybil "github.com/logv/sybil/src/lib"

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Dictionary map[string]interface{}

var JSONPath string

// how many times we try to grab table info when ingesting
var TableInfoGrabs = 10

func ingestDictionary(r *sybil.Record, recordmap *Dictionary, prefix string) {
	for k, v := range *recordmap {
		keyName := fmt.Sprint(prefix, k)
		_, ok := EXCLUDES[keyName]
		if ok {
			continue
		}

		prefixName := fmt.Sprint(keyName, "_")
		switch iv := v.(type) {
		case string:
			if IntCast[keyName] == true {
				val, err := strconv.ParseInt(iv, 10, 64)
				if err == nil {
					r.AddIntField(keyName, int64(val))
				}
			} else {
				r.AddStrField(keyName, iv)

			}
		case int64:
			r.AddIntField(keyName, int64(iv))
		case float64:
			r.AddIntField(keyName, int64(iv))
		// nested fields
		case map[string]interface{}:
			d := Dictionary(iv)
			ingestDictionary(r, &d, prefixName)
		// This is a set field
		case []interface{}:
			keyStrs := make([]string, 0)
			for _, v := range iv {
				switch av := v.(type) {
				case string:
					keyStrs = append(keyStrs, av)
				case float64:
					keyStrs = append(keyStrs, fmt.Sprintf("%.0f", av))
				case int64:
					keyStrs = append(keyStrs, strconv.FormatInt(av, 64))
				}
			}

			r.AddSetField(keyName, keyStrs)
		case nil:
		default:
			sybil.Debug(fmt.Sprintf("TYPE %T IS UNKNOWN FOR FIELD", iv), keyName)
		}
	}
}

func importCSVRecords() {
	// For importing CSV records, we need to validate the headers, then we just
	// read in and fill out record fields!
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	header := scanner.Text()
	headerFields := strings.Split(header, ",")
	sybil.Debug("HEADER FIELDS FOR CSV ARE", headerFields)

	t := sybil.GetTable(*sybil.FLAGS.Table)

	for scanner.Scan() {
		line := scanner.Text()
		r := t.NewRecord()
		fields := strings.Split(line, ",")
		for i, v := range fields {
			fieldName := headerFields[i]

			if v == "" {
				continue
			}

			val, err := strconv.ParseFloat(v, 64)
			if err == nil {
				r.AddIntField(fieldName, int64(val))
			} else {
				r.AddStrField(fieldName, v)
			}

		}

		t.ChunkAndSave()
	}

}

func jsonQuery(obj *interface{}, path []string) []interface{} {

	var ret interface{}
	ret = *obj

	for _, key := range path {
		if key == "$" {
			continue
		}

		switch ing := ret.(type) {
		case map[string]interface{}:
			ret = ing[key]
		case []interface{}:
			// the key should be an integer key...
			intkey, err := strconv.ParseInt(key, 10, 32)
			if err != nil {
				sybil.Debug("USING NON INTEGER KEY TO ACCESS ARRAY!", key, err)
			} else {
				ret = ing[intkey]
			}
		case nil:
			continue
		default:
			sybil.Debug(fmt.Sprintf("DONT KNOW HOW TO ADDRESS INTO OBJ %T", ing))
		}

	}

	switch r := ret.(type) {
	case []interface{}:
		return r
	case map[string]interface{}:
		ret := make([]interface{}, 0)
		ret = append(ret, r)
		return ret
	default:
		sybil.Debug(fmt.Sprintf("RET TYPE %T", r))
	}

	return nil
}

func importJSONRecords() {
	t := sybil.GetTable(*sybil.FLAGS.Table)

	path := strings.Split(JSONPath, ".")
	sybil.Debug("PATH IS", path)

	dec := json.NewDecoder(os.Stdin)

	for {
		var decoded interface{}

		if err := dec.Decode(&decoded); err != nil {
			if err == io.EOF {
				break
			}
			if err != nil {
				sybil.Debug("ERR", err)
			}
		}

		records := jsonQuery(&decoded, path)
		decoded = nil

		for _, ing := range records {
			r := t.NewRecord()
			switch dict := ing.(type) {
			case map[string]interface{}:
				ndict := Dictionary(dict)
				ingestDictionary(r, &ndict, "")
			case Dictionary:
				ingestDictionary(r, &dict, "")

			}
			t.ChunkAndSave()
		}

	}

}

var IntCast = make(map[string]bool)
var EXCLUDES = make(map[string]bool)

func RunIngestCmdLine() {
	ingestfile := flag.String("file", sybil.IngestDir, "name of dir to ingest into")
	fINTS := flag.String("ints", "", "columns to treat as ints (comma delimited)")
	fCSV := flag.Bool("csv", false, "expect incoming data in CSV format")
	fExcludes := flag.String("exclude", "", "Columns to exclude (comma delimited)")
	fJSONPath := flag.String("path", "$", "Path to JSON record, ex: $.foo.bar")
	fSkipCompact := flag.Bool("skip-compact", false, "skip auto compaction during ingest")
	fReopen := flag.String("infile", "", "input file to use (instead of stdin)")
	sybil.FLAGS.SkipCompact = fSkipCompact

	flag.Parse()

	digestfile := fmt.Sprintf("%s", *ingestfile)

	if *sybil.FLAGS.Table == "" {
		flag.PrintDefaults()
		return
	}

	JSONPath = *fJSONPath

	if *fReopen != "" {

		infile, err := os.OpenFile(*fReopen, syscall.O_RDONLY|syscall.O_CREAT, 0666)
		if err != nil {
			sybil.Error("ERROR OPENING INFILE", err)
		}

		os.Stdin = infile

	}

	if *sybil.FLAGS.Profile {
		profile := sybil.RunProfiler()
		defer profile.Start().Stop()
	}

	for _, v := range strings.Split(*fINTS, ",") {
		IntCast[v] = true
	}
	for _, v := range strings.Split(*fExcludes, ",") {
		EXCLUDES[v] = true
	}

	for k := range EXCLUDES {
		sybil.Debug("EXCLUDING COLUMN", k)
	}

	t := sybil.GetTable(*sybil.FLAGS.Table)

	// We have 5 tries to load table info, just in case the lock is held by
	// someone else
	var loadedTable = false
	for i := 0; i < TableInfoGrabs; i++ {
		loaded := t.LoadTableInfo()
		if loaded == true || t.HasFlagFile() == false {
			loadedTable = true
			break
		}
		time.Sleep(time.Millisecond * 10)
	}

	if loadedTable == false {
		if t.HasFlagFile() {
			sybil.Warn("INGESTOR COULDNT READ TABLE INFO, LOSING SAMPLES")
			return
		}
	}

	if *fCSV == false {
		importJSONRecords()
	} else {
		importCSVRecords()
	}

	t.IngestRecords(digestfile)
}
