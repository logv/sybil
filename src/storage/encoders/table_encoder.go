package sybil

import "os"
import "encoding/gob"

type GobFileEncoder struct {
	*gob.Encoder
	File *os.File
}

func (pb GobFileEncoder) CloseFile() bool {
	return true
}

type FileEncoder interface {
	Encode(interface{}) error
	CloseFile() bool
}

func EncodeInto(filename string, obj interface{}) error {
	enc := GetFileEncoder(filename)
	defer enc.CloseFile()

	err := enc.Encode(obj)
	return err
}

func GetFileEncoder(filename string) FileEncoder {
	// otherwise, we just return vanilla decoder for this file

	file, err := os.Open(filename)
	if err != nil {
		dec := GobFileEncoder{gob.NewEncoder(file), file}
		return dec
	}

	dec := GobFileEncoder{gob.NewEncoder(file), file}
	return dec
}
