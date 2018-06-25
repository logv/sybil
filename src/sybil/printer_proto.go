package sybil

import (
	"os"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

func PrintBytesProto(obj proto.Message) error {
	buf, err := proto.Marshal(obj)
	if err != nil {
		return errors.Wrap(err, "encoding protobuf")
	}
	_, err = os.Stdout.Write(buf)
	return err
}
