package sybil

import "fmt"

import "os"
import "strings"
import "encoding/gob"
import "compress/gzip"

func getCompressedDecoder(filename string) *gob.Decoder {

	var dec *gob.Decoder

	file, err := os.Open(filename)
	if err != nil {
		Debug("COULDNT OPEN GZ", filename)
		return gob.NewDecoder(file)
	}

	reader, err := gzip.NewReader(file)
	if err != nil {
		Debug("COULDNT DECOMPRESS GZ", filename)
		return gob.NewDecoder(reader)
	}

	dec = gob.NewDecoder(reader)
	return dec
}

func GetFileDecoder(filename string) *gob.Decoder {
	// if the file ends with GZ ext, we use compressed decoder
	if strings.HasSuffix(filename, GZIP_EXT) {
		return getCompressedDecoder(filename)
	}

	file, err := os.Open(filename)
	// if we try to open the file and its missing, maybe there is a .gz version of it
	if err != nil {
		zfilename := fmt.Sprintf("%s%s", filename, GZIP_EXT)
		_, err = os.Open(zfilename)

		// if we can open this file, we return compressed file decoder
		if err == nil {
			return getCompressedDecoder(zfilename)
		}
	}

	// otherwise, we just return vanilla decoder for this file
	return gob.NewDecoder(file)

}
