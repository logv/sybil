package sybil

import "os"
import "bytes"
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

func encodeInto(filename string, obj interface{}) error {
	var network bytes.Buffer
	// Create an encoder and send a value.
	enc := gob.NewEncoder(&network)
	err := enc.Encode(obj)
	if err != nil {
		Error("encode:", err)
	}

	w, _ := os.Create(filename)

	network.WriteTo(w)
	return err
}
