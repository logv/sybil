package api

import "testing"

import "os"
import "fmt"

var TEST_DB = "testdb"

// Helper for re-creating the test db
func deleteTestDB() {
	Debug("REMOVING TEST DB", TEST_DB)
	os.RemoveAll(fmt.Sprintf("%s", TEST_DB))
}

func TestMain(m *testing.M) {
	deleteTestDB()
	os.Exit(m.Run())
}

func TestQueryTable(t *testing.T) {
	config := SybilConfig{Dir: TEST_DB, Table: "test_query"}
	table := NewTable(&config)

	num_samples := 500

	records := genJSONRecords(num_samples)
	table.AddJSONRecords(records)
	table.FlushRecords()

	sq := table.Query().Limit(num_samples)
	res, err := sq.Execute()

	if err != nil {
		t.Error("Error querying table!")
	}

	if res[0]["Count"] != float64(num_samples) {
		t.Error("Read wrong number of Count back")
	}
	if res[0]["Samples"] != float64(num_samples) {
		t.Error("Read wrong number of Samples back")
	}

}

func TestQuerySamples(t *testing.T) {
	config := SybilConfig{Dir: TEST_DB, Table: "test_query"}
	table := NewTable(&config)
	records := genJSONRecords(1000)
	table.AddJSONRecords(records)
	table.FlushRecords()

	sq := table.Query()
	sq.Samples().
		Limit(1000).
		IntFilterGt("age", 50)

	res, err := sq.Execute()

	if err != nil {
		t.Error("Couldn't query test_query table", err)
	}

	for _, r := range res {
		_, ok := r["abc"]
		if !ok {
			t.Error("MISSING FIELD `abc` IN", r)
		}

		age, ok := r.Int("age")
		if !ok || age < 50 {
			t.Error("AGE CAME BACK SMALLER THAN EXPECTED BASED ON FILTERS: ", r)
			break
		}

		name, ok := r.Str("name")
		if !ok || name == "" {
			t.Error("MISSING NAME FROM SAMPLE", r)
			break
		}
	}

}

func TestTableInfo(t *testing.T) {
	config := SybilConfig{Dir: TEST_DB, Table: "test_info"}
	table := NewTable(&config)
	records := genJSONRecords(1000)
	table.AddJSONRecords(records)
	table.FlushRecords()

	table_info := table.GetTableInfo()
	if table_info.Columns.Ints[0] != "age" {
		t.Error("READ BACK WRONG TABLE INFO", table_info)
	}

	if table_info.Columns.Strs[0] != "name" {
		t.Error("READ BACK WRONG TABLE INFO", table_info)
	}
	if table_info.Columns.Sets[0] != "abc" {
		t.Error("READ BACK WRONG TABLE INFO", table_info)

	}

}

func TestIngestJSON(t *testing.T) {
	config := SybilConfig{Dir: TEST_DB, Table: "test_json"}
	table := NewTable(&config)

	records := genJSONRecords(1000)
	table.AddJSONRecords(records)

	table.FlushRecords()

	sq := table.Query()
	res, err := sq.Samples().
		Limit(1000).
		Execute()

	if err != nil {
		t.Error("COULDNT READ SAMPLES BACK FROM TABLE")
	}

	if len(res) != 1000.0 {
		t.Error("READ LESS SAMPLES THAN INGESTED VIA JSON", len(res), "EXPECTED", 1000)
	}

}

func TestIngestStruct(t *testing.T) {
	config := SybilConfig{Dir: TEST_DB, Table: "test_structs"}
	table := NewTable(&config)

	records := genStructRecords(1000)
	table.AddRecords(records)

	table.FlushRecords()

	sq := table.Query()
	res, err := sq.Samples().
		Limit(1000).
		Execute()

	if err != nil {
		t.Error("COULDNT READ SAMPLES BACK FROM TABLE")
	}

	if len(res) != 1000.0 {
		t.Error("READ LESS SAMPLES THAN INGESTED VIA JSON")
	}
}

func TestIngestJSONBytes(t *testing.T) {
	config := SybilConfig{Dir: TEST_DB, Table: "test_json_bytes"}
	table := NewTable(&config)

	record := []byte(`{"page": 1, "fruits": ["apple", "peach"]}`)
	records := [][]byte{record}
	table.AddRecords(records)

	table.FlushRecords()

	sq := table.Query()
	res, err := sq.Samples().
		Limit(1000).
		Execute()

	if err != nil {
		t.Error("COULDNT READ SAMPLES BACK FROM TABLE")
	}

	Debug("RES", res)
	if len(res) != 1.0 {
		t.Error("READ LESS SAMPLES THAN INGESTED VIA JSON")
	}
}

func TestIngestSybilMap(t *testing.T) {
	config := SybilConfig{Dir: TEST_DB, Table: "test_sybil_map"}
	table := NewTable(&config)

	record := SybilMapRecord{}
	record["name"] = "foobar"
	record["age"] = 123
	record["friends"] = []string{"peter", "paul", "mary"}

	records := make([]SybilMapRecord, 0)
	records = append(records, record)

	table.AddRecords(records)

	table.FlushRecords()

	sq := table.Query()
	res, err := sq.Samples().
		Limit(1000).
		Execute()

	if err != nil {
		t.Error("COULDNT READ SAMPLES BACK FROM TABLE")
	}

	Debug("RES", res)
	if len(res) != 1.0 {
		t.Error("READ LESS SAMPLES THAN INGESTED VIA JSON")
	}
}

func TestIngestMap(t *testing.T) {
	config := SybilConfig{Dir: TEST_DB, Table: "test_map"}
	table := NewTable(&config)

	record := SybilMapRecord{}
	record["name"] = "foobar"
	record["age"] = 123
	record["friends"] = []string{"peter", "paul", "mary"}

	records := make([]map[string]interface{}, 0)
	records = append(records, record)

	table.AddRecords(records)

	table.FlushRecords()

	sq := table.Query()
	res, err := sq.Samples().
		Limit(1000).
		Execute()

	if err != nil {
		t.Error("COULDNT READ SAMPLES BACK FROM TABLE")
	}

	Debug("RES", res)
	if len(res) != 1.0 {
		t.Error("READ LESS SAMPLES THAN INGESTED VIA JSON")
	}
}

func TestListTables(t *testing.T) {
	deleteTestDB()

	config := SybilConfig{Dir: TEST_DB, Table: "test_structs"}
	table := NewTable(&config)

	records := genStructRecords(1)
	table.AddRecords(records)
	table.FlushRecords()
	tables := ListTables(&config)

	if tables[0] != config.Table {
		t.Error("MISSING test_structs TABLE AFTER CREATING IT")
	}
}

func TestDigestTable(t *testing.T) {
	config := SybilConfig{Dir: TEST_DB, Table: "test_digest"}
	table := NewTable(&config)

	num_samples := 500
	records := genJSONRecords(num_samples)
	table.AddJSONRecords(records)

	table.FlushRecords()
	sq := table.Query().Limit(num_samples).ReadRowLog(false)
	res, err := sq.Execute()
	if err != nil {
		t.Error("ERROR WHILE QUERYING TABLE", err)
	}

	if len(res) > 0 {
		t.Error("QUERY RETURNED RESULTS INCORRECTLY, SHOULD HAVE RETURNED EMPTY")
	}

	table.DigestRecords()
	res, err = sq.Execute()
	if err != nil {
		t.Error("ERROR WHILE QUERYING TABLE", err)
	}

	if res[0]["Count"] != float64(num_samples) {
		t.Error("QUERY RETURNED INCORRECT RESULT COUNT")
	}
}

// NOT YET IMPLEMENTED
func testQueryTimeSeries(t *testing.T) {

}

func testTrimTable(t *testing.T) {

}

func testQueryTableGroupBy(t *testing.T) {

}

func testQueryTableHists(t *testing.T) {

}
