package sybild

import (
	"bytes"
	"io"
	"os"
	"os/exec"
)

func sybilIngest(tableName string, buf io.Reader) error {
	const sybilBinary = "sybil"
	var sybilFlags = []string{"ingest", "-table", tableName}
	c := exec.Command(sybilBinary, sybilFlags...)
	c.Stderr = os.Stderr
	si, err := c.StdinPipe()
	if err != nil {
		return err
	}
	so, err := c.StdoutPipe()
	if err != nil {
		return err
	}
	if err := c.Start(); err != nil {
		return err
	}
	io.Copy(si, buf)
	if err := si.Close(); err != nil {
		return err
	}
	if err := c.Wait(); err != nil {
		return err
	}
	results := new(bytes.Buffer)
	io.Copy(results, so)
	return nil
}
