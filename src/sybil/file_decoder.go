package sybil

import (
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"os"
	"strings"
)

var GOB_GZIP_EXT = ".db.gz"

type GobFileDecoder struct {
	*gob.Decoder
	File *os.File
}

type FileDecoder interface {
	Decode(interface{}) error
	CloseFile() error
}

func (gfd GobFileDecoder) CloseFile() error {
	return gfd.File.Close()
}

func decodeInto(filename string, obj interface{}) error {
	dec, err := GetFileDecoder(filename)
	if err != nil {
		return err
	}

	if err := dec.Decode(obj); err != nil {
		return err
	}
	return dec.CloseFile()
}

func getGobGzipDecoder(filename string) (FileDecoder, error) {

	var dec *gob.Decoder

	file, err := os.Open(filename)
	if err != nil {
		Debug("COULDNT OPEN GZ", filename)
		return nil, err
	}

	reader, err := gzip.NewReader(file)
	if err != nil {
		Debug("COULDNT DECOMPRESS GZ", filename)
		return nil, err
	}

	dec = gob.NewDecoder(reader)
	return GobFileDecoder{dec, file}, nil
}

func GetFileDecoder(filename string) (FileDecoder, error) {
	// if the file ends with GZ ext, we use compressed decoder
	if strings.HasSuffix(filename, GOB_GZIP_EXT) {
		return getGobGzipDecoder(filename)
	}

	file, err := os.Open(filename)
	// if we try to open the file and its missing, maybe there is a .gz version of it
	if err != nil {
		zfilename := fmt.Sprintf("%s%s", filename, GZIP_EXT)
		_, err := os.Open(zfilename)

		// if we can open this file, we return compressed file decoder
		if err == nil {
			if strings.HasSuffix(zfilename, GOB_GZIP_EXT) {
				return getGobGzipDecoder(zfilename)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	// otherwise, we just return vanilla decoder for this file
	dec := GobFileDecoder{gob.NewDecoder(file), file}
	return dec, nil

}
