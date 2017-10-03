package sybil

import "io"
import "os"
import "strings"
import "encoding/gob"

import "github.com/DeDiS/protobuf"

type GobFileEncoder struct {
	*gob.Encoder
	File *os.File
}

func (pb GobFileEncoder) CloseFile() bool {
	return true
}

type ProtobufEncoder struct {
	File io.Writer
}

func (pb ProtobufEncoder) Encode(obj interface{}) error {
	buf, err := protobuf.Encode(obj)
	if err != nil {
		return err
	}

	pb.File.Write(buf)
	return nil

}

func (pb ProtobufEncoder) CloseFile() bool {
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

	if strings.HasSuffix(filename, PROTOBUF_EXT) {
		dec := ProtobufEncoder{file}
		return dec
	}

	dec := GobFileEncoder{gob.NewEncoder(file), file}
	return dec
}
