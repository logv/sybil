package sybil

import (
	"encoding/gob"
	"flag"
	"io/ioutil"
	"os"

	"github.com/gogo/protobuf/proto"
	"github.com/logv/sybil/src/internal/internalpb"
	"github.com/pkg/errors"
)

func init() {
	setDefaults()
}

var TEST_MODE = false

type StrReplace struct {
	Pattern string
	Replace string
}

var FLAGS = internalpb.FlagDefs{}

func setDefaults() {
	FLAGS.GC = true
	FLAGS.PRINT = true

	FLAGS.LOAD_AND_QUERY = true
	flag.StringVar(&FLAGS.DIR, "dir", "./db/", "Directory to store DB files")
	flag.StringVar(&FLAGS.TABLE, "table", "", "Table to operate on [REQUIRED]")

	flag.BoolVar(&FLAGS.DEBUG, "debug", false, "enable debug logging")
	flag.StringVar(&FLAGS.FIELD_SEPARATOR, "field-separator", ",", "Field separator used in command line params")
	flag.StringVar(&FLAGS.FILTER_SEPARATOR, "filter-separator", ":", "Filter separator used in filters")

	FLAGS.SKIP_OUTLIERS = true
	FLAGS.RECYCLE_MEM = true

	FLAGS.LIMIT = 100

	if PROFILER_ENABLED {
		flag.BoolVar(&FLAGS.PROFILE, "profile", false, "turn profiling on?")
		flag.BoolVar(&FLAGS.PROFILE_MEM, "mem", false, "turn memory profiling on")
	}

	FLAGS.WRITE_BLOCK_INFO = false

}

func EncodeFlags() error {
	oldEncode := FLAGS.ENCODE_FLAGS
	FLAGS.ENCODE_FLAGS = false
	if FLAGS.PROTO {
		Debug("ENCODING FLAGS AS PROTO")
		if err := PrintBytesProto(&FLAGS); err != nil {
			return errors.Wrap(err, "encoding flags as proto")
		}
	} else {
		if err := PrintBytes(FLAGS); err != nil {
			return err
		}
	}
	FLAGS.ENCODE_FLAGS = oldEncode
	return nil
}

func DecodeFlags() error {
	Debug("READING ENCODED FLAGS FROM STDIN")
	dec := gob.NewDecoder(os.Stdin)
	return dec.Decode(&FLAGS)
}

func DecodeFlagsProto() error {
	Debug("READING ENCODED FLAGS FROM STDIN AS PROTO")
	data, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return errors.Wrap(err, "read stdin")
	}
	err = proto.Unmarshal(data, &FLAGS)
	return errors.Wrap(err, "proto.Unmarshal")
}
