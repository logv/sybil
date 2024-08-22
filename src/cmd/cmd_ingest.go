package sybil_cmd

import sybil "github.com/logv/sybil/src/lib"

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
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

var JSON_PATH string

// how many times we try to grab table info when ingesting
var TABLE_INFO_GRABS = 10

func float64ToBytes(f float64) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, f)
	return buf.Bytes()
}

func bytesToInt64(b []byte) int64 {
	var i int64
	buf := bytes.NewReader(b)
	binary.Read(buf, binary.LittleEndian, &i)
	return i
}

func bytesToFloat64(b []byte) float64 {
	var i float64
	buf := bytes.NewReader(b)
	binary.Read(buf, binary.LittleEndian, &i)
	return i
}

func ingest_dictionary(r *sybil.Record, recordmap *Dictionary, prefix string, timestampFormat string) {
	for k, v := range *recordmap {
		key_name := fmt.Sprint(prefix, k)
		_, ok := EXCLUDES[key_name]
		if ok {
			continue
		}

		prefix_name := fmt.Sprint(key_name, "_")
		switch iv := v.(type) {
		case string:
			if TIMESTAMPS[key_name] {
				t, err := time.Parse(timestampFormat, iv)
				if err != nil {
					sybil.Debug(fmt.Sprintf("PROBLEM PARSING '%v' as '%v'", iv, timestampFormat), key_name)
					continue
				}
				r.AddIntField(key_name, t.Local().Unix())
				continue
			}
			if INT_CAST[key_name] {
				val, err := strconv.ParseInt(iv, 10, 64)
				if err != nil {
					sybil.Debug(fmt.Sprintf("PROBLEM PARSING '%v' as int", iv), key_name)
					continue
				}
				r.AddIntField(key_name, int64(val))
			} else {
				r.AddStrField(key_name, iv)

			}
		case json.Number:
			iiv, err := iv.Int64()
			if err == nil {
				r.AddIntField(key_name, iiv)
				continue
			}

			fv, err := iv.Float64()
			if err == nil {
				r.AddFloatField(key_name, fv)
				continue
			}

		case float64:
			r.AddFloatField(key_name, iv)
		case bool:
			if iv {
				r.AddIntField(key_name, 1)
			} else {
				r.AddIntField(key_name, 0)
			}
		// nested fields
		case map[string]interface{}:
			d := Dictionary(iv)
			ingest_dictionary(r, &d, prefix_name, timestampFormat)
		// This is a set field
		case []interface{}:
			key_strs := make([]string, 0)
			for _, v := range iv {
				switch av := v.(type) {
				case string:
					key_strs = append(key_strs, av)
				case float64:
					key_strs = append(key_strs, fmt.Sprintf("%.0f", av))
				case int64:
					key_strs = append(key_strs, strconv.FormatInt(av, 64))
				}
			}

			r.AddSetField(key_name, key_strs)
		case nil:
		default:
			sybil.Debug(fmt.Sprintf("TYPE %T IS UNKNOWN FOR FIELD", iv), key_name)
		}
	}
}

var IMPORTED_COUNT = 0

func import_csv_records() {
	// For importing CSV records, we need to validate the headers, then we just
	// read in and fill out record fields!
	scanner := csv.NewReader(os.Stdin)
	header_fields, err := scanner.Read()
	if err == nil {
		sybil.Debug("HEADER FIELDS FOR CSV ARE", header_fields)
	} else {
		sybil.Error("ERROR READING CSV HEADER", err)
	}

	t := sybil.GetTable(sybil.FLAGS.TABLE)

	for {
		fields, err := scanner.Read()
		if err == io.EOF {
			break
		}

		if err, ok := err.(*csv.ParseError); ok && err.Err != csv.ErrFieldCount {
			sybil.Warn("ERROR READING LINE", err, fields)
			continue
		}

		r := t.NewRecord()
		for i, v := range fields {
			if i >= len(header_fields) {
				continue
			}

			field_name := header_fields[i]

			if v == "" {
				continue
			}

			val, err := strconv.ParseFloat(v, 64)
			if err == nil {
				r.AddIntField(field_name, int64(val))
			} else {
				r.AddStrField(field_name, v)
			}

		}

		t.ChunkAndSave()
	}

}

func json_query(obj *interface{}, path []string) []interface{} {

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

func import_json_records(timestampFormat string) {
	t := sybil.GetTable(sybil.FLAGS.TABLE)

	path := strings.Split(JSON_PATH, ".")
	sybil.Debug("PATH IS", path)

	dec := json.NewDecoder(os.Stdin)
	dec.UseNumber()

	for {
		var decoded interface{}
		if err := dec.Decode(&decoded); err == io.EOF {
			break
		} else if err != nil {
			sybil.Debug("ERR", err)
		}

		records := json_query(&decoded, path)
		decoded = nil

		for _, ing := range records {
			r := t.NewRecord()
			switch dict := ing.(type) {
			case map[string]interface{}:
				ndict := Dictionary(dict)
				ingest_dictionary(r, &ndict, "", timestampFormat)
			case Dictionary:
				ingest_dictionary(r, &dict, "", timestampFormat)

			}
			t.ChunkAndSave()
		}

	}

}

var INT_CAST = make(map[string]bool)
var TIMESTAMPS = make(map[string]bool)
var EXCLUDES = make(map[string]bool)

func RunIngestCmdLine() {
	ingestfile := flag.String("file", sybil.INGEST_DIR, "name of dir to ingest into")
	f_INTS := flag.String("ints", "", "columns to treat as ints (comma delimited)")
	f_CSV := flag.Bool("csv", false, "expect incoming data in CSV format")
	f_EXCLUDES := flag.String("exclude", "", "Columns to exclude (comma delimited)")
	f_JSON_PATH := flag.String("path", "$", "Path to JSON record, ex: $.foo.bar")
	flag.BoolVar(&sybil.FLAGS.SKIP_COMPACT, "skip-compact", false, "skip auto compaction during ingest")

	flag.BoolVar(&sybil.FLAGS.SAVE_AS_SRB, "save-srb", false, "Save ingestion records as SaveRecordBlocks, including Key info")
	f_REOPEN := flag.String("infile", "", "input file to use (instead of stdin)")
	f_TIMESTAMPS := flag.String("timestamps", "", "columns to treat as ints (comma delimited), parsed via timestamp-format")
	f_TIMESTAMP_FORMAT := flag.String("timestamp-format", time.RFC3339, "when -timestamps is provided, this is the parsing string used")

	flag.Parse()

	digestfile := fmt.Sprintf("%s", *ingestfile)

	if sybil.FLAGS.TABLE == "" {
		flag.PrintDefaults()
		return
	}

	JSON_PATH = *f_JSON_PATH

	if *f_REOPEN != "" {

		infile, err := os.OpenFile(*f_REOPEN, syscall.O_RDONLY|syscall.O_CREAT, 0666)
		if err != nil {
			sybil.Error("ERROR OPENING INFILE", err)
		}

		os.Stdin = infile

	}

	if sybil.FLAGS.PROFILE {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	for _, v := range strings.Split(*f_TIMESTAMPS, ",") {
		TIMESTAMPS[v] = true
	}
	for _, v := range strings.Split(*f_INTS, ",") {
		INT_CAST[v] = true
	}
	for _, v := range strings.Split(*f_EXCLUDES, ",") {
		EXCLUDES[v] = true
	}

	for k, _ := range EXCLUDES {
		sybil.Debug("EXCLUDING COLUMN", k)
	}

	t := sybil.GetTable(sybil.FLAGS.TABLE)

	// We have 5 tries to load table info, just in case the lock is held by
	// someone else
	var loaded_table = false
	for i := 0; i < TABLE_INFO_GRABS; i++ {
		loaded := t.LoadTableInfo()
		if loaded == true || t.HasFlagFile() == false {
			loaded_table = true
			break
		}
		time.Sleep(time.Millisecond * 10)
	}

	if loaded_table == false {
		if t.HasFlagFile() {
			sybil.Warn("INGESTOR COULDNT READ TABLE INFO, LOSING SAMPLES")
			return
		}
	}

	if *f_CSV == false {
		import_json_records(*f_TIMESTAMP_FORMAT)
	} else {
		import_csv_records()
	}

	t.IngestRecords(digestfile)
}
