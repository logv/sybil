package sybil

import "testing"
import "math/rand"
import "strconv"

type loadColCB func(*LoadSpec)

func TestSkipChangedBlocks(t *testing.T) {
	tableName := getTestTableName(t)
	deleteTestDb(tableName)

	blockCount := 5

	var thisAddRecords = func(block_count int) {
		addRecords(tableName, func(r *Record, i int) {
			age := int64(rand.Intn(20)) + 10

			ageStr := strconv.FormatInt(int64(age), 10)
			r.AddIntField("id", int64(i))
			r.AddIntField("age", age)
			r.AddStrField("age_str", ageStr)
			r.AddSetField("age_set", []string{ageStr})

		}, block_count)
		saveAndReloadTable(t, tableName, block_count)

	}

	thisAddRecords(blockCount)

	nt := GetTable(tableName)

	for key := range nt.BlockList {
		block := nt.BlockList[key]
		block.RecordList = block.RecordList[:10]
		block.SaveInfoToColumns(block.Name)
		break
	}

	// Testing for int
	var testReadBlocks = func(cb loadColCB) {
		expectedCount := CHUNK_SIZE * (blockCount - 1)
		tbl := GetTable(tableName)

		tbl.SaveRecordsToColumns()

		unloadTestTable(tableName)

		nt = GetTable(tableName)
		nt.LoadTableInfo()

		loadSpec := NewLoadSpec()
		cb(&loadSpec)
		count := nt.LoadRecords(&loadSpec)

		if count != expectedCount {
			t.Error("Wrote", expectedCount, "records, but read back", count)
		}

		// +1 is the Row Store Block...
		if len(nt.BlockList) != blockCount {
			t.Error("Wrote", blockCount, "blocks, but came back with", len(nt.BlockList))
		}

	}

	testReadBlocks(func(loadSpec *LoadSpec) {
		loadSpec.Int("id")
	})
	testReadBlocks(func(loadSpec *LoadSpec) {
		loadSpec.Str("age_str")
	})
	testReadBlocks(func(loadSpec *LoadSpec) {
		loadSpec.Set("age_set")
	})

	deleteTestDb(tableName)
}

func TestSkipChangedBlocksLargeChunk(t *testing.T) {
	old_chunk_size := CHUNK_SIZE
	CHUNK_SIZE = CARDINALITY_THRESHOLD + 1
	tableName := getTestTableName(t)
	deleteTestDb(tableName)

	blockCount := 2

	var thisAddRecords = func(block_count int) {
		addRecords(tableName, func(r *Record, i int) {
			age := int64(rand.Intn(20)) + 10

			ageStr := strconv.FormatInt(int64(age), 10)
			r.AddIntField("id", int64(i))
			r.AddIntField("age", age)
			r.AddStrField("age_str", ageStr)
			r.AddSetField("age_set", []string{ageStr})

		}, block_count)
		saveAndReloadTable(t, tableName, block_count)

	}

	thisAddRecords(blockCount)

	nt := GetTable(tableName)

	for key := range nt.BlockList {
		block := nt.BlockList[key]
		block.RecordList = block.RecordList[:10]
		block.SaveInfoToColumns(block.Name)
		break
	}

	// Testing for int
	var testReadBlocks = func(cb loadColCB) {
		expectedCount := CHUNK_SIZE * (blockCount - 1)
		tbl := GetTable(tableName)

		tbl.SaveRecordsToColumns()

		unloadTestTable(tableName)

		nt = GetTable(tableName)
		nt.LoadTableInfo()

		loadSpec := NewLoadSpec()
		cb(&loadSpec)
		count := nt.LoadRecords(&loadSpec)

		if count != expectedCount {
			t.Error("Wrote", expectedCount, "records, but read back", count)
		}

		// +1 is the Row Store Block...
		if len(nt.BlockList) != blockCount {
			t.Error("Wrote", blockCount, "blocks, but came back with", len(nt.BlockList))
		}

	}

	testReadBlocks(func(loadSpec *LoadSpec) {
		loadSpec.Int("id")
	})
	testReadBlocks(func(loadSpec *LoadSpec) {
		loadSpec.Str("age_str")
	})
	testReadBlocks(func(loadSpec *LoadSpec) {
		loadSpec.Set("age_set")
	})

	deleteTestDb(tableName)
	CHUNK_SIZE = old_chunk_size
}
