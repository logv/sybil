package sybil

import "io/ioutil"
import "os"
import "path"
import "encoding/gob"

type NodeResults struct {
	Table     Table
	Tables    []string
	QuerySpec QuerySpec
	Samples   []*Sample
}
type VTable struct {
	Table

	Columns map[string]*IntInfo
}

func (vt *VTable) findResultsInDirs(dirs []string) map[string]*NodeResults {
	all_specs := make(map[string]*NodeResults)
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

			var node_results NodeResults
			if err != nil {
				Debug("DECODE ERROR", err)
				continue
			}

			err = dec.Decode(&node_results)

			if err == nil {
				cs := NodeResults(node_results)
				all_specs[f.Name()] = &node_results
				Debug("DECODED QUERY RESULTS FROM", fname)
				Debug("QUERY SPEC CACHE KEY IS", cs.QuerySpec.GetCacheKey(NULL_BLOCK))
			} else {
				Debug("DECODE ERROR", err)
			}

		}

	}

	return all_specs

}

func (vt *VTable) AggregateSamples(dirs []string) {
	Debug("AGGREGATING TABLE LIST")
	all_results := vt.findResultsInDirs(dirs)

	limit := FLAGS.LIMIT

	samples := make([]*Sample, 0)

	for _, res := range all_results {
		for _, s := range res.Samples {
			samples = append(samples, s)
		}
	}

	if len(samples) > limit {
		samples = samples[:limit]
	}

	// TODO: call into vt.PrintSamples later after adjusting how we store the samples
	// on a per table basis
	printJson(samples)

}

func (vt *VTable) AggregateTables(dirs []string) {
	Debug("AGGREGATING TABLE LIST")
	all_results := vt.findResultsInDirs(dirs)
	Debug("FOUND", len(all_results), "SPECS TO AGG")

	all_tables := make(map[string]int, 0)

	for _, res := range all_results {
		for _, table := range res.Tables {
			count, ok := all_tables[table]
			if !ok {
				count = 0
			}
			all_tables[table] = count + 1
		}
	}

	table_arr := make([]string, 0)
	for table := range all_tables {
		table_arr = append(table_arr, table)
	}

	printTablesToOutput(table_arr)
}

func (vt *VTable) AggregateInfo(dirs []string) {
	// TODO: combine all result info
	Debug("AGGREGATING TABLE INFO LIST")
	all_results := vt.findResultsInDirs(dirs)

	count := 0
	size := int64(0)

	for res_name, res := range all_results {
		for _, block := range res.Table.BlockList {
			count += int(block.Info.NumRecords)
			size += block.Size
		}

		res.Table.BlockList = make(map[string]*TableBlock, 0)

		res.Table.init_locks()
		res.Table.populate_string_id_lookup()

		virtual_block := TableBlock{}
		virtual_block.Size = size
		saved_info := SavedColumnInfo{NumRecords: int32(count)}
		virtual_block.Info = &saved_info

		vt.BlockList[res_name] = &virtual_block

		for name_id, key_type := range res.Table.KeyTypes {
			key_name := res.Table.get_string_for_key(int(name_id))
			this_id := vt.get_key_id(key_name)

			vt.set_key_type(this_id, key_type)
		}

	}

	vt.PrintTableInfo()

}

func (vt *VTable) AggregateSpecs(dirs []string) {
	Debug("AGGREGATING QUERY RESULTS")

	// TODO: verify all specs have the same md5 key
	all_results := vt.findResultsInDirs(dirs)
	Debug("FOUND", len(all_results), "SPECS TO AGG")

	var qs QuerySpec
	for _, res := range all_results {
		qs = res.QuerySpec
		break
	}

	all_specs := make(map[string]*QuerySpec)
	for k, v := range all_results {
		all_specs[k] = &v.QuerySpec
	}

	final_result := QuerySpec{}
	final_result.Punctuate()
	final_result.QueryParams = qs.QueryParams

	FLAGS.OP = HIST_STR
	OPTS.MERGE_TABLE = &vt.Table

	combined_result := CombineResults(&final_result, all_specs)
	combined_result.QueryParams = qs.QueryParams

	combined_result.SortResults(combined_result.OrderBy, combined_result.OrderAsc)
	combined_result.PrintResults()
}

func (vt *VTable) StitchResults(dirs []string) {
	vt.init_data_structures()

	if FLAGS.LIST_TABLES == true {
		vt.AggregateTables(dirs)
		return
	}

	if FLAGS.PRINT_INFO {
		vt.AggregateInfo(dirs)
		return
	}

	if FLAGS.SAMPLES {
		vt.AggregateSamples(dirs)
		return
	}

	vt.AggregateSpecs(dirs)
}
