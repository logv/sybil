package sybil

import "encoding/gob"
import "crypto/md5"
import "bytes"
import "fmt"
import "path"
import "io/ioutil"
import "os"
import "compress/gzip"

func init() {
	registerTypesForQueryCache()
}

// this registration is used for saving and decoding cached per block query
// results
func registerTypesForQueryCache() {
	gob.Register(IntFilter{})
	gob.Register(StrFilter{})
	gob.Register(SetFilter{})

	gob.Register(IntField(0))
	gob.Register(StrField(0))
	gob.Register(SetField{})
	gob.Register(&HistCompat{})
	gob.Register(&MultiHistCompat{})
}

func (t *Table) getCachedQueryForBlock(dirname string, querySpec *QuerySpec) (*TableBlock, *QuerySpec) {

	if *FLAGS.CACHED_QUERIES == false {
		return nil, nil
	}

	tb := newTableBlock()
	tb.Name = dirname
	tb.table = t
	info := t.LoadBlockInfo(dirname)

	if info == nil {
		Debug("NO INFO FOR", dirname)
		return nil, nil
	}

	if info.NumRecords <= 0 {
		Debug("NO RECORDS FOR", dirname)
		return nil, nil
	}

	tb.Info = info

	blockQuery := CopyQuerySpec(querySpec)
	if blockQuery.LoadCachedResults(tb.Name) {
		t.blockM.Lock()
		t.BlockList[dirname] = &tb
		t.blockM.Unlock()

		return &tb, blockQuery

	}

	return nil, nil

}

// for a per block query cache, we exclude any trivial filters (that are true
// for all records in the block) when creating our cache key
func (querySpec *QuerySpec) GetCacheRelevantFilters(blockname string) []Filter {

	filters := make([]Filter, 0)
	if querySpec == nil {
		return filters
	}

	t := querySpec.Table

	info := t.LoadBlockInfo(blockname)

	if info == nil {
		return filters
	}

	maxRecord := Record{Ints: IntArr{}, Strs: StrArr{}}
	minRecord := Record{Ints: IntArr{}, Strs: StrArr{}}

	if len(info.IntInfoMap) == 0 {
		return filters
	}

	for fieldName := range info.StrInfoMap {
		fieldID := t.getKeyID(fieldName)
		minRecord.ResizeFields(fieldID)
		maxRecord.ResizeFields(fieldID)
	}

	for fieldName, fieldInfo := range info.IntInfoMap {
		fieldID := t.getKeyID(fieldName)
		minRecord.ResizeFields(fieldID)
		maxRecord.ResizeFields(fieldID)

		minRecord.Ints[fieldID] = IntField(fieldInfo.Min)
		maxRecord.Ints[fieldID] = IntField(fieldInfo.Max)

		minRecord.Populated[fieldID] = INT_VAL
		maxRecord.Populated[fieldID] = INT_VAL
	}

	for _, f := range querySpec.Filters {
		// make the minima record and the maxima records...
		switch fil := f.(type) {
		case IntFilter:
			// we only use block extents for skipping gt and lt filters
			if fil.Op != "lt" && fil.Op != "gt" {
				filters = append(filters, f)
				continue
			}

			if f.Filter(&minRecord) && f.Filter(&maxRecord) {
			} else {
				filters = append(filters, f)
			}

		default:
			filters = append(filters, f)
		}
	}

	return filters

}

func (qs *QuerySpec) GetCacheStruct(blockname string) QueryParams {
	cacheSpec := QueryParams(qs.QueryParams)

	// kick out trivial filters
	cacheSpec.Filters = qs.GetCacheRelevantFilters(blockname)

	return cacheSpec
}

func (qs *QuerySpec) GetCacheKey(blockname string) string {
	cacheSpec := qs.GetCacheStruct(blockname)

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(cacheSpec)
	if err != nil {
		Warn("encode:", err)
		return ""
	}

	h := md5.New()
	h.Write(buf.Bytes())

	ret := fmt.Sprintf("%x", h.Sum(nil))
	return ret
}

func (qs *QuerySpec) LoadCachedResults(blockname string) bool {
	if *FLAGS.CACHED_QUERIES == false {
		return false
	}

	if *FLAGS.SAMPLES {
		return false

	}

	cacheKey := qs.GetCacheKey(blockname)

	cacheDir := path.Join(blockname, "cache")
	cacheName := fmt.Sprintf("%s.db", cacheKey)
	filename := path.Join(cacheDir, cacheName)

	cachedSpec := QueryResults{}
	err := decodeInto(filename, &cachedSpec)

	if err != nil {
		return false
	}

	qs.QueryResults = cachedSpec

	return true
}

func (qs *QuerySpec) SaveCachedResults(blockname string) {
	if *FLAGS.CACHED_QUERIES == false {
		return
	}

	if *FLAGS.SAMPLES {
		return
	}

	info := qs.Table.LoadBlockInfo(blockname)

	if info.NumRecords < int32(CHUNK_SIZE) {
		return
	}

	cacheKey := qs.GetCacheKey(blockname)

	cachedInfo := qs.QueryResults

	cacheDir := path.Join(blockname, "cache")
	err := os.MkdirAll(cacheDir, 0777)
	if err != nil {
		Debug("COULDNT CREATE CACHE DIR", err, "NOT CACHING QUERY")
		return
	}

	cacheName := fmt.Sprintf("%s.db.gz", cacheKey)
	filename := path.Join(cacheDir, cacheName)
	tempfile, err := ioutil.TempFile(cacheDir, cacheName)
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
