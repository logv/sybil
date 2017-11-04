package sybil

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/structs"

	md "github.com/logv/sybil/src/lib/metadata"
	filters "github.com/logv/sybil/src/query/filters"
	hists "github.com/logv/sybil/src/query/hists"
	specs "github.com/logv/sybil/src/query/specs"
	encoders "github.com/logv/sybil/src/storage/encoders"
	md_io "github.com/logv/sybil/src/storage/metadata_io"
)

// this registration is used for saving and decoding cached per block query
// results
func RegisterTypesForQueryCache() {
	gob.Register(filters.IntFilter{})
	gob.Register(filters.StrFilter{})
	gob.Register(filters.SetFilter{})
	gob.Register(&hists.HistCompat{})
	gob.Register(&hists.MultiHistCompat{})
}

func GetCachedQueryForBlock(t *Table, dirname string, querySpec *specs.QuerySpec) (*TableBlock, *specs.QuerySpec) {

	if *FLAGS.CACHED_QUERIES == false {
		return nil, nil
	}

	tb := NewTableBlock()
	tb.Name = dirname
	tb.Table = t
	info := md_io.LoadBlockInfo(t, dirname)

	if info == nil {
		Debug("NO INFO FOR", dirname)
		return nil, nil
	}

	if info.NumRecords <= 0 {
		Debug("NO RECORDS FOR", dirname)
		return nil, nil
	}

	tb.Info = info

	blockQuery := specs.CopyQuerySpec(querySpec)
	if LoadCachedResults(blockQuery, tb.Name) {
		t.BlockMutex.Lock()
		t.BlockList[dirname] = &tb
		t.BlockMutex.Unlock()

		return &tb, blockQuery

	}

	return nil, nil

}

// for a per block query cache, we exclude any trivial filters (that are true
// for all records in the block) when creating our cache key
func GetCacheRelevantFilters(querySpec *specs.QuerySpec, blockname string) []Filter {

	relevant_filters := make([]Filter, 0)
	if querySpec == nil {
		return relevant_filters
	}

	t := querySpec.Table

	info := md_io.LoadBlockInfo(t, blockname)

	max_record := Record{Ints: IntArr{}, Strs: StrArr{}}
	min_record := Record{Ints: IntArr{}, Strs: StrArr{}}

	if len(info.IntInfoMap) == 0 {
		return relevant_filters
	}

	for field_name := range info.StrInfoMap {
		field_id := md.GetTableKeyID(t, field_name)
		ResizeFields(&min_record, field_id)
		ResizeFields(&max_record, field_id)
	}

	for field_name, field_info := range info.IntInfoMap {
		field_id := md.GetTableKeyID(t, field_name)
		ResizeFields(&min_record, field_id)
		ResizeFields(&max_record, field_id)

		min_record.Ints[field_id] = IntField(field_info.Min)
		max_record.Ints[field_id] = IntField(field_info.Max)

		min_record.Populated[field_id] = INT_VAL
		max_record.Populated[field_id] = INT_VAL
	}

	for _, f := range querySpec.Filters {
		// make the minima record and the maxima records...
		switch fil := f.(type) {
		case filters.IntFilter:
			// we only use block extents for skipping gt and lt relevant_filters
			if fil.Op != "lt" && fil.Op != "gt" {
				relevant_filters = append(relevant_filters, f)
				continue
			}

			if f.Filter(&min_record) && f.Filter(&max_record) {
			} else {
				relevant_filters = append(relevant_filters, f)
			}

		default:
			relevant_filters = append(relevant_filters, f)
		}
	}

	return relevant_filters

}

func GetCacheStruct(qs *specs.QuerySpec, blockname string) specs.QueryParams {
	cache_spec := specs.QueryParams(qs.QueryParams)

	// kick out trivial filters
	cache_spec.Filters = GetCacheRelevantFilters(qs, blockname)

	return cache_spec
}

func GetCacheKey(qs *specs.QuerySpec, blockname string) string {
	cache_spec := GetCacheStruct(qs, blockname)

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(cache_spec)
	if err != nil {
		Warn("encode:", err)
		return ""
	}

	h := md5.New()
	h.Write(buf.Bytes())

	ret := fmt.Sprintf("%x", h.Sum(nil))
	return ret
}

func LoadCachedResults(qs *specs.QuerySpec, blockname string) bool {
	if *FLAGS.CACHED_QUERIES == false {
		return false
	}

	if *FLAGS.SAMPLES {
		return false

	}

	cache_key := GetCacheKey(qs, blockname)

	cache_dir := path.Join(blockname, "cache")
	cache_name := fmt.Sprintf("%s.db", cache_key)
	filename := path.Join(cache_dir, cache_name)

	cachedSpec := specs.QueryResults{}
	err := encoders.DecodeInto(filename, &cachedSpec)

	if err != nil {
		return false
	}

	qs.QueryResults = cachedSpec

	return true
}

func SaveCachedResults(qs *specs.QuerySpec, blockname string) {
	if *FLAGS.CACHED_QUERIES == false {
		return
	}

	if *FLAGS.SAMPLES {
		return
	}

	info := md_io.LoadBlockInfo(qs.Table, blockname)

	if info.NumRecords < int32(CHUNK_SIZE) {
		return
	}

	cache_key := GetCacheKey(qs, blockname)

	cachedInfo := qs.QueryResults

	cache_dir := path.Join(blockname, "cache")
	err := os.MkdirAll(cache_dir, 0777)
	if err != nil {
		Debug("COULDNT CREATE CACHE DIR", err, "NOT CACHING QUERY")
		return
	}

	cache_name := fmt.Sprintf("%s.db.gz", cache_key)
	filename := path.Join(cache_dir, cache_name)
	tempfile, err := ioutil.TempFile(cache_dir, cache_name)
	if err != nil {
		Debug("TEMPFILE ERROR", err)
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(cachedInfo)

	var gbuf bytes.Buffer
	w := gzip.NewWriter(&gbuf)
	w.Write(buf.Bytes())
	w.Close() // You must close this first to flush the bytes to the buffer.

	if err != nil {
		Warn("cached query encoding error:", err)
		return
	}

	if err != nil {
		Warn("ERROR CREATING TEMP FILE FOR QUERY CACHED INFO", err)
		return
	}

	_, err = gbuf.WriteTo(tempfile)
	if err != nil {
		Warn("ERROR SAVING QUERY CACHED INFO INTO TEMPFILE", err)
		return
	}

	tempfile.Close()
	err = RenameAndMod(tempfile.Name(), filename)
	if err != nil {
		Warn("ERROR RENAMING", tempfile.Name())
	}

	return

}

func WriteQueryCache(t *Table, to_cache_specs map[string]*specs.QuerySpec) {

	// NOW WE SAVE OUR QUERY CACHE HERE...
	savestart := time.Now()

	if *FLAGS.CACHED_QUERIES {
		for blockName, blockQuery := range to_cache_specs {

			if blockName == INGEST_DIR {
				continue
			}

			SaveCachedResults(blockQuery, blockName)
			if *FLAGS.DEBUG {
				fmt.Fprint(os.Stderr, "s")
			}
		}

		saveend := time.Now()

		if len(to_cache_specs) > 0 {
			if *FLAGS.DEBUG {
				fmt.Fprint(os.Stderr, "\n")
			}
			Debug("SAVING CACHED QUERIES TOOK", saveend.Sub(savestart))
		}
	}

	// END QUERY CACHE SAVING

}

func init() {
	RegisterTypesForQueryCache()
}
