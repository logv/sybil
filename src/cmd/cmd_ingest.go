package cmd

import (
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

	"github.com/logv/sybil/src/sybil"
	"github.com/pkg/errors"
)

type Dictionary map[string]interface{}

// how many times we try to grab table info when ingesting
var TABLE_INFO_GRABS = 10

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
			if INT_CAST[keyName] {
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

var IMPORTED_COUNT = 0

func importCsvRecords() error {
	// For importing CSV records, we need to validate the headers, then we just
	// read in and fill out record fields!
	scanner := csv.NewReader(os.Stdin)
	headerFields, err := scanner.Read()
	if err == nil {
		sybil.Debug("HEADER FIELDS FOR CSV ARE", headerFields)
	} else {
		return errors.Wrap(err, "error reading csv header")
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
			if i >= len(headerFields) {
				continue
			}

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

		if err := t.ChunkAndSave(); err != nil {
			return err
		}
	}
	return nil
}

func jsonQuery(obj *interface{}, path []string) []interface{} {
	ret := *obj

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

func importJSONRecords(jsonPath string) error {
	t := sybil.GetTable(sybil.FLAGS.TABLE)

	path := strings.Split(jsonPath, ".")
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
			if err := t.ChunkAndSave(); err != nil {
				return err
			}
		}

	}

	return nil
}

var INT_CAST = make(map[string]bool)
var EXCLUDES = make(map[string]bool)

func RunIngestCmdLine() {
	ingestfile := flag.String("file", sybil.INGEST_DIR, "name of dir to ingest into")
	fInts := flag.String("ints", "", "columns to treat as ints (comma delimited)")
	fCsv := flag.Bool("csv", false, "expect incoming data in CSV format")
	fExcludes := flag.String("exclude", "", "Columns to exclude (comma delimited)")
	fJSONPath := flag.String("path", "$", "Path to JSON record, ex: $.foo.bar")
	fReopen := flag.String("infile", "", "input file to use (instead of stdin)")
	flag.BoolVar(&sybil.FLAGS.SKIP_COMPACT, "skip-compact", false, "skip auto compaction during ingest")

	flag.Parse()
	if err := runIngestCmdLine(&sybil.FLAGS, *ingestfile, *fInts, *fCsv, *fExcludes, *fJSONPath, *fReopen); err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrap(err, "ingest"))
		os.Exit(1)
	}
}

func runIngestCmdLine(flags *sybil.FlagDefs, digestFile string, ints string, csv bool, excludes string, jsonPath string, filePath string) error {

	if flags.TABLE == "" {
		flag.PrintDefaults()
		return sybil.ErrMissingTable
	}

	if filePath != "" {

		infile, err := os.OpenFile(filePath, syscall.O_RDONLY|syscall.O_CREAT, 0666)
		if err != nil {
			return errors.Wrap(err, "error opening infile")
		}

		os.Stdin = infile

	}

	if flags.PROFILE {
		profile := sybil.RUN_PROFILER()
		defer profile.Start().Stop()
	}

	for _, v := range strings.Split(ints, ",") {
		INT_CAST[v] = true
	}
	for _, v := range strings.Split(excludes, ",") {
		EXCLUDES[v] = true
	}

	for k := range EXCLUDES {
		sybil.Debug("EXCLUDING COLUMN", k)
	}

	t := sybil.GetTable(flags.TABLE)

	// We have 5 tries to load table info, just in case the lock is held by
	// someone else
	var loadedTable = false
	var loadErr error
	for i := 0; i < TABLE_INFO_GRABS; i++ {
		loadErr = t.LoadTableInfo()
		if loadErr == nil || !t.HasFlagFile() {
			loadedTable = true
			break
		}
		time.Sleep(time.Millisecond * 10)
	}

	if !loadedTable {
		if t.HasFlagFile() {
			sybil.Warn("INGESTOR COULDNT READ TABLE INFO, LOSING SAMPLES")
			if loadErr == nil {
				loadErr = fmt.Errorf("unknown (nil) error")
			}
			return errors.Wrap(loadErr, "issue loading existing table")
		}
	}

	var err error
	if !csv {
		err = importJSONRecords(jsonPath)
	} else {
		err = importCsvRecords()
	}
	if err != nil {
		return err
	}
	return t.IngestRecords(digestFile)
}
