package edb_test

import edb "../"

import "math/rand"
import "testing"

func TestTableLoadRecords(test *testing.T) {
	delete_test_db()


	if testing.Short() {
		test.Skip("Skipping test in short mode")
		return
	}

	BLOCK_COUNT := 3
	COUNT := edb.CHUNK_SIZE * BLOCK_COUNT
	t := edb.GetTable(TEST_TABLE_NAME)

	for i := 0; i < COUNT; i++ {
		r := t.NewRecord()
		r.AddIntField("id", i)
		r.AddIntField("age", int(rand.Intn(50)) + 10)
	}

	t.SaveRecords()

	unload_test_table()

	nt := edb.GetTable(TEST_TABLE_NAME)
	loadSpec := edb.NewLoadSpec()
	loadSpec.LoadAllColumns = true
	count := nt.LoadRecords(&loadSpec)

	if count != COUNT {
		test.Error("Wrote 100 records, but read back", count)
	}

	if len(nt.BlockList) != BLOCK_COUNT {
		test.Error("Wrote 2 blocks, but more came back")
	}

}

