package sybil

import "testing"
import "fmt"
import "time"
import "os"
import "math/rand"
import "strconv"

func TestTableCreate(t *testing.T) {
	t.Parallel()
	flags := DefaultFlags()
	tableName := getTestTableName(t)
	deleteTestDb(tableName)
	defer deleteTestDb(tableName)

	blockCount := 3
	created := addRecords(tableName, func(r *Record, index int) {
		r.AddIntField(flags, "id", int64(index))
		age := int64(rand.Intn(20)) + 10
		r.AddIntField(flags, "age", age)
		r.AddStrField("age_str", strconv.FormatInt(int64(age), 10))
		r.AddIntField(flags, "time", int64(time.Now().Unix()))
		r.AddStrField("name", fmt.Sprint("user", index))
	}, blockCount)

	nt := saveAndReloadTable(t, flags, tableName, blockCount)

	if nt.Name != tableName {
		t.Error("TEST TABLE NAME INCORRECT")
	}

	nt.LoadTableInfo(flags)

	_, err := os.Open(fmt.Sprintf("db/%s/info.db", tableName))
	if err != nil {
		fmt.Println("ERR", err)
		t.Error("Test table did not create info.db")
	}

	loadSpec := nt.NewLoadSpec()
	loadSpec.LoadAllColumns = true

	nt.LoadRecords(flags, &loadSpec)

	var records = make([]*Record, 0)
	for _, b := range nt.BlockList {
		records = append(records, b.RecordList...)
	}

	if len(records) != len(created) {
		t.Error("More records were created than expected", len(records))
	}
}
