package sybil

import "io/ioutil"
import "os"
import "path"
import "encoding/gob"

var MERGE_TABLE Table

type VTable struct {
	Table

	Columns map[string]*IntInfo
}

func (vt *VTable) findSpecsInDirs(dirs []string) map[string]*QuerySpec {
	all_specs := make(map[string]*QuerySpec)
	for _, d := range dirs {
		files, err := ioutil.ReadDir(d)
		if err != nil {
			Debug("COULDNT READ DIR", d, "ERR: ", err)
			continue
		}

		for _, f := range files {
			fname := path.Join(d, f.Name())
			fd, err := os.Open(fname)
			dec := gob.NewDecoder(fd)
			qs := QuerySpec{}
			if err != nil {
				Debug("DECODE ERROR", err)
				continue
			}

			err = dec.Decode(&qs)

			if err == nil {
				cs := QuerySpec(qs)
				all_specs[f.Name()] = &cs
				Debug("DECODED QUERY RESULTS FROM", fname)
				Debug("QUERY SPEC CACHE KEY IS", cs.GetCacheKey(NULL_BLOCK))
			} else {
				Debug("DECODE ERROR", err)
			}

		}

	}

	return all_specs

}

func (vt *VTable) AggregateDirs(dirs []string) {
	// TODO: verify all specs have the same md5 key

	var qs QuerySpec

	all_specs := vt.findSpecsInDirs(dirs)
	for _, spec := range all_specs {
		qs = *spec
		break
	}

	final_result := QuerySpec{}
	final_result.Punctuate()
	final_result.QueryParams = qs.QueryParams

	FLAGS.OP = &HIST_STR
	OPTS.MERGE_TABLE = &vt.Table

	combined_result := CombineResults(&final_result, all_specs)
	combined_result.QueryParams = qs.QueryParams

	SortResults(combined_result)
	PrintResults(combined_result)
}
