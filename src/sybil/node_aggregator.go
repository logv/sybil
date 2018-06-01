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
	allSpecs := make(map[string]*NodeResults)
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

			var nodeResults NodeResults
			if err != nil {
				Debug("DECODE ERROR", err)
				continue
			}

			err = dec.Decode(&nodeResults)

			if err == nil {
				cs := NodeResults(nodeResults)
				allSpecs[f.Name()] = &nodeResults
				Debug("DECODED QUERY RESULTS FROM", fname)
				Debug("QUERY SPEC CACHE KEY IS", cs.QuerySpec.GetCacheKey(NULL_BLOCK))
			} else {
				Debug("DECODE ERROR", err)
			}

		}

	}

	return allSpecs

}

func (vt *VTable) AggregateSamples(dirs []string) {
	Debug("AGGREGATING TABLE LIST")
	allResults := vt.findResultsInDirs(dirs)

	limit := *FLAGS.LIMIT

	samples := make([]*Sample, 0)

	for _, res := range allResults {
		samples = append(samples, res.Samples...)
	}

	if len(samples) > limit {
		samples = samples[:limit]
	}

	// TODO: call into vt.PrintSamples later after adjusting how we store the samples
	// on a per table basis
	printJSON(samples)

}

func (vt *VTable) AggregateTables(dirs []string) {
	Debug("AGGREGATING TABLE LIST")
	allResults := vt.findResultsInDirs(dirs)
	Debug("FOUND", len(allResults), "SPECS TO AGG")

	allTables := make(map[string]int, 0)

	for _, res := range allResults {
		for _, table := range res.Tables {
			count, ok := allTables[table]
			if !ok {
				count = 0
			}
			allTables[table] = count + 1
		}
	}

	tableArr := make([]string, 0)
	for table := range allTables {
		tableArr = append(tableArr, table)
	}

	printTablesToOutput(tableArr)
}

func (vt *VTable) AggregateInfo(dirs []string) {
	// TODO: combine all result info
	Debug("AGGREGATING TABLE INFO LIST")
	allResults := vt.findResultsInDirs(dirs)

	count := 0
	size := int64(0)

	for resName, res := range allResults {
		for _, block := range res.Table.BlockList {
			count += int(block.Info.NumRecords)
			size += block.Size
		}

		res.Table.BlockList = make(map[string]*TableBlock, 0)

		res.Table.initLocks()
		res.Table.populateStringIDLookup()

		virtualBlock := TableBlock{}
		virtualBlock.Size = size
		savedInfo := SavedColumnInfo{NumRecords: int32(count)}
		virtualBlock.Info = &savedInfo

		vt.BlockList[resName] = &virtualBlock

		for nameID, keyType := range res.Table.KeyTypes {
			keyName := res.Table.getStringForKey(int(nameID))
			thisID := vt.getKeyID(keyName)

			vt.setKeyType(thisID, keyType)
		}

	}

	vt.PrintColInfo()

}

func (vt *VTable) AggregateSpecs(dirs []string) {
	Debug("AGGREGATING QUERY RESULTS")

	// TODO: verify all specs have the same md5 key
	allResults := vt.findResultsInDirs(dirs)
	Debug("FOUND", len(allResults), "SPECS TO AGG")

	var qs QuerySpec
	for _, res := range allResults {
		qs = res.QuerySpec
		break
	}

	allSpecs := make(map[string]*QuerySpec)
	for k, v := range allResults {
		allSpecs[k] = &v.QuerySpec
	}

	finalResult := QuerySpec{}
	finalResult.Punctuate()
	finalResult.QueryParams = qs.QueryParams

	FLAGS.OP = &HIST_STR
	OPTS.MERGE_TABLE = &vt.Table

	combinedResult := CombineResults(&finalResult, allSpecs)
	combinedResult.QueryParams = qs.QueryParams

	combinedResult.SortResults(combinedResult.OrderBy)
	combinedResult.PrintResults()
}

func (vt *VTable) StitchResults(dirs []string) {
	vt.initDataStructures()

	if FLAGS.LIST_TABLES != nil && *FLAGS.LIST_TABLES {
		vt.AggregateTables(dirs)
		return
	}

	if FLAGS.PRINT_INFO != nil && *FLAGS.PRINT_INFO {
		vt.AggregateInfo(dirs)
		return
	}

	if FLAGS.SAMPLES != nil && *FLAGS.SAMPLES {
		vt.AggregateSamples(dirs)
		return
	}

	vt.AggregateSpecs(dirs)
}
