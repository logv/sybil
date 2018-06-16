// +build gofuzz,go1.7

package cmd

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

const corpusPath = "../../workdir/query/corpus"

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
	t.Log(len(files), "corpus files at", corpusPath)
	for _, f := range files {
		t.Run(f, func(t *testing.T) {
			b, err := ioutil.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			t.Log(FuzzQuery(b))
			time.Sleep(time.Millisecond * 100)
		})
	}
}
