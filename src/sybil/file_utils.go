package sybil

import "os"
import "io"

// FOUND AT
// https://gist.github.com/elazarl/5507969
func cp(dst, src string) error {
	s, err := os.Open(src)
	if err != nil {
		return err
	}
	// no need to check errors on read only file, we already got everything
	// we need from the filesystem, so nothing can go wrong now.
	defer s.Close()
	d, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(d, s); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}

// TODO: We should really split this into two functions based on dir / file
func RenameAndMod(src, dst string) error {
	os.Chmod(src, 0755)
	return os.Rename(src, dst)
}
