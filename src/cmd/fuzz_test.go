// +build gofuzz

package cmd

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

const corpusPath = "../../workdir/corpus"

var fuzzPrep = `
rm -rf ./db
python ../../scripts/fakedata/host_generator.py | sybil ingest -table a
sybil digest -table a
`

func TestFuzzes(t *testing.T) {
	cmd := exec.Command("bash", "-c", fuzzPrep)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(corpusPath, "*"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		t.Run(f, func(t *testing.T) {
			b, err := ioutil.ReadFile(filepath.Join(corpusPath, f))
			if err != nil {
				t.Fatal(err)
			}
			t.Log(Fuzz(b))
		})
	}
}
