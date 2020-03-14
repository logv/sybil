package sybil

import "testing"
import "math/rand"
import "strconv"
import "strings"
import "math"

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
		loadSpec.LoadAllColumns = true
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
		loadSpec.LoadAllColumns = true
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

// Tests that the average histogram works
func TestShortenKeyTable(t *testing.T) {
	tableName := getTestTableName(t)
	deleteTestDb(tableName)

	if testing.Short() {
		t.Skip("Skipping test in short mode")
		return
	}

	blockCount := 3

	totalAge := int64(0)
	count := 0
	addRecords(tableName, func(r *Record, index int) {
		count++
		r.AddIntField("id", int64(index))
		age := int64(rand.Intn(20)) + 10
		totalAge += age
		r.AddIntField("age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
	}, blockCount)

	avgAge := float64(totalAge) / float64(count)

	nt := saveAndReloadTable(t, tableName, blockCount)

	// We have to unload and reload the table if we are doing a shorten key test
	unloadTestTable(tableName)

	nt = GetTable(tableName)
	nt.LoadTableInfo()

	nt.UseKeys([]string{"age"})
	nt.ShortenKeyTable()

	querySpec := newQuerySpec()
	querySpec.Aggregations = append(querySpec.Aggregations, nt.Aggregation("age", "avg"))

	if len(nt.KeyTable) != 1 {
		t.Error("KEY TABLE SHORTENING DIDNT WORK")
	}

	Debug("KEY TABLES", nt.KeyTable, nt.KeyTypes)
	Debug("INFO MAP", nt.IntInfo)

	nt.MatchAndAggregate(querySpec)

	for k, v := range querySpec.Results {
		k = strings.Replace(k, GROUP_DELIMITER, "", 1)
		Debug("AVG AGE", avgAge)

		if math.Abs(float64(avgAge)-float64(v.Hists["age"].Mean())) > 0.1 {
			t.Error("GROUP BY YIELDED UNEXPECTED RESULTS", k, avgAge, v.Hists["age"].Mean())
		}
	}
	deleteTestDb(tableName)

}
