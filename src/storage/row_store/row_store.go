package sybil

import (
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/config"
	md "github.com/logv/sybil/src/lib/metadata"
	record "github.com/logv/sybil/src/lib/record"
	. "github.com/logv/sybil/src/lib/structs"
	aggregate "github.com/logv/sybil/src/query/aggregate"
	specs "github.com/logv/sybil/src/query/specs"
	encoders "github.com/logv/sybil/src/storage/encoders"
	flock "github.com/logv/sybil/src/storage/file_locks"
)

type RowSavedInt struct {
	Name  int16
	Value int64
}

type RowSavedStr struct {
	Name  int16
	Value string
}

type RowSavedSet struct {
	Name  int16
	Value []string
}

type SavedRecord struct {
	Ints []RowSavedInt
	Strs []RowSavedStr
	Sets []RowSavedSet
}

type AfterRowBlockLoad func(string, RecordList)

func LoadRowStoreRecords(t *Table, digest string, after_block_load_cb AfterRowBlockLoad) {
	dirname := path.Join(*FLAGS.DIR, t.Name, digest)
	var err error

	// if the row store dir does not exist, skip the whole function
	_, err = os.Stat(dirname)
	if os.IsNotExist(err) {
		if after_block_load_cb != nil {
			after_block_load_cb(NO_MORE_BLOCKS, nil)
		}

		return
	}

	var file *os.File
	for i := 0; i < flock.LOCK_TRIES; i++ {
		file, err = os.Open(dirname)
		if err != nil {
			Debug("Can't open the ingestion dir", dirname)
			time.Sleep(flock.LOCK_US)
			if i > MAX_ROW_STORE_TRIES {
				return
			}
			continue
		}
		break
	}

	files, err := file.Readdir(0)
	if t.RowBlock == nil {
		t.RowBlock = &TableBlock{}
		(*t.RowBlock).RecordList = make(RecordList, 0)
		t.RowBlock.Info = &SavedColumnInfo{}
		t.BlockMutex.Lock()
		t.BlockList[ROW_STORE_BLOCK] = t.RowBlock
		t.BlockMutex.Unlock()
		t.RowBlock.Name = ROW_STORE_BLOCK
	}

	for _, file := range files {
		filename := file.Name()

		// we can open .gz files as well as regular .db files
		cname := strings.TrimRight(filename, GZIP_EXT)

		if strings.HasSuffix(cname, ".db") == false {
			continue
		}

		filename = path.Join(dirname, file.Name())

		records := LoadRecordsFromLog(t, filename)
		if after_block_load_cb != nil {
			after_block_load_cb(filename, records)
		}
	}

	if after_block_load_cb != nil {
		after_block_load_cb(NO_MORE_BLOCKS, nil)
	}

}

func LoadRecordsFromLog(t *Table, filename string) RecordList {
	var marshalled_records []*SavedRecord

	// Create an encoder and send a value.
	err := encoders.DecodeInto(filename, &marshalled_records)
	if err != nil {
		Debug("ERROR LOADING INGESTION LOG", err)
	}

	ret := make(RecordList, len(marshalled_records))

	for i, r := range marshalled_records {
		ret[i] = r.toRecord(t)
	}
	return ret

}
func (s SavedRecord) toRecord(t *Table) *Record {
	r := Record{}
	r.Ints = IntArr{}
	r.Strs = StrArr{}
	r.SetMap = SetMap{}

	b := t.LastBlock
	t.LastBlock.RecordList = append(t.LastBlock.RecordList, &r)

	b.Table = t
	r.Block = &b

	max_key_id := 0
	for _, v := range t.KeyTable {
		if max_key_id <= int(v) {
			max_key_id = int(v) + 1
		}
	}

	ResizeFields(&r, int16(max_key_id))

	for _, v := range s.Ints {
		r.Populated[v.Name] = INT_VAL
		r.Ints[v.Name] = IntField(v.Value)
		md.UpdateTableIntInfo(t, v.Name, v.Value)
	}

	for _, v := range s.Strs {
		record.AddStrField(&r, md.GetTableStringForKey(t, int(v.Name)), v.Value)
	}

	for _, v := range s.Sets {
		record.AddSetField(&r, md.GetTableStringForKey(t, int(v.Name)), v.Value)
		r.Populated[v.Name] = SET_VAL
	}

	return &r
}

func ToSavedRecord(r *Record) *SavedRecord {
	s := SavedRecord{}
	for k, v := range r.Ints {
		if r.Populated[k] == INT_VAL {
			s.Ints = append(s.Ints, RowSavedInt{int16(k), int64(v)})
		}
	}

	for k, v := range r.Strs {
		if r.Populated[k] == STR_VAL {
			col := md.GetColumnInfo(r.Block, int16(k))
			str_val := md.GetColumnStringForVal(col, int32(v))
			s.Strs = append(s.Strs, RowSavedStr{int16(k), str_val})
		}
	}

	for k, v := range r.SetMap {
		if r.Populated[k] == SET_VAL {
			col := md.GetColumnInfo(r.Block, int16(k))
			set_vals := make([]string, len(v))
			for i, val := range v {
				set_vals[i] = md.GetColumnStringForVal(col, int32(val))
			}
			s.Sets = append(s.Sets, RowSavedSet{int16(k), set_vals})
		}
	}

	return &s

}

type SavedRecords struct {
	RecordList []*SavedRecord
}

type AfterLoadQueryCB struct {
	QuerySpec *specs.QuerySpec
	WG        *sync.WaitGroup
	Records   RecordList

	Count int
}

func (cb *AfterLoadQueryCB) CB(digestname string, records RecordList) {
	if digestname == NO_MORE_BLOCKS {
		// TODO: add sessionization call over here, too
		count := aggregate.FilterAndAggRecords(cb.QuerySpec, &cb.Records)
		cb.Count += count

		cb.WG.Done()
		return
	}

	querySpec := cb.QuerySpec

	for _, r := range records {
		add := true
		// FILTERING
		for j := 0; j < len(querySpec.Filters); j++ {
			// returns True if the record matches!
			ret := querySpec.Filters[j].Filter(r) != true
			if ret {
				add = false
				break
			}
		}

		if !add {
			continue
		}

		cb.Records = append(cb.Records, r)
	}

	if *FLAGS.DEBUG {
		fmt.Fprint(os.Stderr, "+")
	}
}
