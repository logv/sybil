package sybil

import (
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	. "github.com/logv/sybil/src/lib/structs"

	specs "github.com/logv/sybil/src/query/specs"
	encoders "github.com/logv/sybil/src/storage/encoders"
	flock "github.com/logv/sybil/src/storage/file_locks"
	md_io "github.com/logv/sybil/src/storage/metadata_io"
)

var CHUNKS_BEFORE_GC = 16
var BLOCKS_PER_CACHE_FILE = 64

type LoadRecordsFunc func(t *Table, loadSpec *specs.LoadSpec) int

func BlankLoadRecords(t *Table, loadSpec *specs.LoadSpec) int {

	return 0
}

var LOAD_RECORDS_FUNC = BlankLoadRecords

func SetTableQueryFunc(qf LoadRecordsFunc) {
	LOAD_RECORDS_FUNC = qf
}

func GetNewIngestBlockName(t *Table) (string, error) {
	Debug("GETTING INGEST BLOCK NAME", *FLAGS.DIR, "TABLE", t.Name)
	name, err := ioutil.TempDir(path.Join(*FLAGS.DIR, t.Name), "block")
	return name, err
}

func saveRecordList(t *Table, records RecordList) bool {
	if len(records) == 0 {
		return false
	}

	Debug("SAVING RECORD LIST", len(records), t.Name)

	chunk_size := CHUNK_SIZE
	chunks := len(records) / chunk_size

	if chunks == 0 {
		filename, err := GetNewIngestBlockName(t)
		if err != nil {
			Error("ERR SAVING BLOCK", filename, err)
		}
		SaveRecordsToBlock(t, records, filename)
	} else {
		for j := 0; j < chunks; j++ {
			filename, err := GetNewIngestBlockName(t)
			if err != nil {
				Error("ERR SAVING BLOCK", filename, err)
			}
			SaveRecordsToBlock(t, records[j*chunk_size:(j+1)*chunk_size], filename)
		}

		// SAVE THE REMAINDER
		if len(records) > chunks*chunk_size {
			filename, err := GetNewIngestBlockName(t)
			if err != nil {
				Error("Error creating new ingestion block", err)
			}

			SaveRecordsToBlock(t, records[chunks*chunk_size:], filename)
		}
	}

	return true
}

func SaveRecordsToColumns(t *Table) bool {
	os.MkdirAll(path.Join(*FLAGS.DIR, t.Name), 0777)
	sort.Sort(SortRecordsByTime{t.NewRecords})

	FillPartialBlock(t)
	ret := saveRecordList(t, t.NewRecords)
	t.NewRecords = make(RecordList, 0)
	md_io.SaveTableInfo(t, "info")

	return ret

}

func FileLooksLikeBlock(v os.FileInfo) bool {

	switch {

	case v.Name() == INGEST_DIR || v.Name() == TEMP_INGEST_DIR:
		return false
	case v.Name() == CACHE_DIR:
		return false
	case strings.HasPrefix(v.Name(), STOMACHE_DIR):
		return false
	case strings.HasSuffix(v.Name(), "info.db"):
		return false
	case strings.HasSuffix(v.Name(), "old"):
		return false
	case strings.HasSuffix(v.Name(), "broken"):
		return false
	case strings.HasSuffix(v.Name(), "lock"):
		return false
	case strings.HasSuffix(v.Name(), "export"):
		return false
	case strings.HasSuffix(v.Name(), "partial"):
		return false
	}

	return true

}

func getNewCacheBlockFile(t *Table) (*os.File, error) {
	Debug("GETTING CACHE BLOCK NAME", *FLAGS.DIR, "TABLE", t.Name)
	table_cache_dir := path.Join(*FLAGS.DIR, t.Name, CACHE_DIR)
	os.MkdirAll(table_cache_dir, 0755)

	// this info block needs to be moved once its finished being written to
	file, err := ioutil.TempFile(table_cache_dir, "info")
	return file, err
}

func LoadBlockCache(t *Table) {
	if flock.GrabCacheLock(t) == false {
		return
	}

	defer flock.ReleaseCacheLock(t)
	files, err := ioutil.ReadDir(path.Join(*FLAGS.DIR, t.Name, CACHE_DIR))

	if err != nil {
		return
	}

	for _, block_file := range files {
		filename := path.Join(*FLAGS.DIR, t.Name, CACHE_DIR, block_file.Name())
		block_cache := SavedBlockCache{}
		if err != nil {
			continue
		}

		err = encoders.DecodeInto(filename, &block_cache)
		if err != nil {
			continue
		}

		for k, v := range block_cache {
			t.BlockInfoCache[k] = v
		}
	}

	Debug("FILLED BLOCK CACHE WITH", len(t.BlockInfoCache), "ITEMS")
}

func WriteBlockCache(t *Table) {
	if len(t.NewBlockInfos) == 0 {
		return
	}

	if flock.GrabCacheLock(t) == false {
		return
	}

	defer flock.ReleaseCacheLock(t)

	Debug("WRITING BLOCK CACHE, OUTSTANDING", len(t.NewBlockInfos))

	var num_blocks = len(t.NewBlockInfos) / BLOCKS_PER_CACHE_FILE

	for i := 0; i < num_blocks; i++ {
		cached_info := t.NewBlockInfos[i*BLOCKS_PER_CACHE_FILE : (i+1)*BLOCKS_PER_CACHE_FILE]

		block_file, err := getNewCacheBlockFile(t)
		if err != nil {
			Debug("TROUBLE CREATING CACHE BLOCK FILE")
			break
		}
		block_cache := SavedBlockCache{}

		for _, block_name := range cached_info {
			block_cache[block_name] = t.BlockInfoCache[block_name]
		}

		enc := gob.NewEncoder(block_file)
		err = enc.Encode(&block_cache)
		if err != nil {
			Debug("ERROR ENCODING BLOCK CACHE", err)
		}

		pathname := fmt.Sprintf("%s.db", block_file.Name())

		Debug("RENAMING", block_file.Name(), pathname)
		RenameAndMod(block_file.Name(), pathname)

	}

	t.NewBlockInfos = t.NewBlockInfos[:0]

}

func ChunkAndSave(t *Table) {

	if len(t.NewRecords) >= CHUNK_SIZE {
		os.MkdirAll(path.Join(*FLAGS.DIR, t.Name), 0777)
		name, err := GetNewIngestBlockName(t)
		if err == nil {
			SaveRecordsToBlock(t, t.NewRecords, name)
			md_io.SaveTableInfo(t, "info")
			t.NewRecords = make(RecordList, 0)
			ReleaseRecords(t)
		} else {
			Error("ERROR SAVING BLOCK", err)
		}
	}

}
