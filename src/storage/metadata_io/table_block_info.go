package sybil

import (
	"fmt"
	"time"

	. "github.com/logv/sybil/src/lib/common"
	. "github.com/logv/sybil/src/lib/structs"
	. "github.com/logv/sybil/src/storage/encoders"
)

func LoadBlockInfo(t *Table, dirname string) *SavedColumnInfo {

	t.BlockMutex.Lock()
	cached_info, ok := t.BlockInfoCache[dirname]
	t.BlockMutex.Unlock()
	if ok {
		return cached_info
	}

	// find out how many records are kept in this dir...
	info := SavedColumnInfo{}
	istart := time.Now()
	filename := fmt.Sprintf("%s/info.db", dirname)

	err := DecodeInto(filename, &info)

	if err != nil {
		Warn("ERROR DECODING COLUMN BLOCK INFO!", dirname, err)
		return &info
	}
	iend := time.Now()

	if DEBUG_TIMING {
		Debug("LOAD BLOCK INFO TOOK", iend.Sub(istart))
	}

	t.BlockMutex.Lock()
	t.BlockInfoCache[dirname] = &info
	if info.NumRecords >= int32(CHUNK_SIZE) {
		t.NewBlockInfos = append(t.NewBlockInfos, dirname)
	}
	t.BlockMutex.Unlock()

	return &info
}
