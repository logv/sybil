package common

import "os"

// TODO: We should really split this into two functions based on dir / file
func RenameAndMod(src, dst string) error {
	os.Chmod(src, 0755)
	return os.Rename(src, dst)
}
