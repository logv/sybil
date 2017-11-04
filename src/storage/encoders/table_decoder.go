package sybil

import (
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"os"
	"strings"

	. "github.com/logv/sybil/src/lib/common"
)

var GOB_GZIP_EXT = ".db.gz"

type GobFileDecoder struct {
	*gob.Decoder
	File *os.File
}

func (gfd GobFileDecoder) CloseFile() bool {
	gfd.File.Close()
	return true
}

type FileDecoder interface {
	Decode(interface{}) error
	CloseFile() bool
}

func DecodeInto(filename string, obj interface{}) error {
	dec := GetFileDecoder(filename)
	defer dec.CloseFile()

	err := dec.Decode(obj)
	return err
}

func getGobGzipDecoder(filename string) FileDecoder {

	var dec *gob.Decoder

	file, err := os.Open(filename)
	if err != nil {

		Debug("COULDNT OPEN GZ", filename)
		return GobFileDecoder{gob.NewDecoder(file), file}
	}

	reader, err := gzip.NewReader(file)
	if err != nil {
		Debug("COULDNT DECOMPRESS GZ", filename)
		return GobFileDecoder{gob.NewDecoder(reader), file}
	}

	dec = gob.NewDecoder(reader)
	return GobFileDecoder{dec, file}
}

func GetFileDecoder(filename string) FileDecoder {
	// if the file ends with GZ ext, we use compressed decoder
	// if the file ends with GZ ext, we use compressed decoder
	if strings.HasSuffix(filename, GOB_GZIP_EXT) {
		dec := getGobGzipDecoder(filename)
		return dec
	}

	file, err := os.Open(filename)
	// if we try to open the file and its missing, maybe there is a .gz version of it
	if err != nil {
		zfilename := fmt.Sprintf("%s%s", filename, GZIP_EXT)
		_, err = os.Open(zfilename)

		// if we can open this file, we return compressed file decoder
		if err == nil {
			if strings.HasSuffix(zfilename, GOB_GZIP_EXT) {
				dec := getGobGzipDecoder(zfilename)
				return dec
			}

		}
	}

	// otherwise, we just return vanilla decoder for this file
	dec := GobFileDecoder{gob.NewDecoder(file), file}
	return dec

}
