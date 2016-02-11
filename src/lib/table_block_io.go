package pcs

import "fmt"
import "bytes"
import "log"
import "os"
import "encoding/gob"
import "strings"
import "sort"
import "time"

type ValueMap map[int64][]uint32

func delta_encode_col(col ValueMap) {
	for _, records := range col {
		prev := uint32(0)
		for i, v := range records {
			records[int32(i)] = v - prev
			prev = v

		}
	}
}

func delta_encode(same_map map[int16]ValueMap) {
	for _, col := range same_map {
		if len(col) <= CHUNK_SIZE/10 {
			delta_encode_col(col)
		}
	}
}

// this is used to record the buckets when building the column
// blobs
func record_value(same_map map[int16]ValueMap, index int32, name int16, value int64) {
	s, ok := same_map[name]
	if !ok {
		same_map[name] = ValueMap{}
		s = same_map[name]
	}

	vi := value

	s[vi] = append(s[vi], uint32(index))
}

func (tb *TableBlock) getColumnInfo(name_id int16) *TableColumn {
	col, ok := tb.columns[name_id]
	if !ok {
		col = tb.newTableColumn()
		tb.columns[name_id] = col
	}

	return col
}

func (tb *TableBlock) SaveIntsToColumns(dirname string, same_ints map[int16]ValueMap) {
	// now make the dir and shoot each blob out into a separate file

	// SAVED TO A SINGLE BLOCK ON DISK, NOW TO SAVE IT OUT TO SEPARATE VALUES
	os.MkdirAll(dirname, 0777)
	for k, v := range same_ints {
		col_name := tb.get_string_for_key(k)
		if col_name == "" {
			log.Println("CANT FIGURE OUT FIELD NAME FOR", k, "SOMETHING IS PROBABLY AWRY")
			continue
		}
		intCol := SavedIntColumn{}
		intCol.NameId = k
		intCol.Name = col_name
		intCol.DeltaEncodedIDs = DELTA_ENCODE_RECORD_IDS

		count := 0
		record_to_value := make(map[uint32]int64)
		for bucket, records := range v {
			si := SavedIntBucket{Value: bucket, Records: records}
			intCol.Bins = append(intCol.Bins, si)
			count += len(records)
			for _, r := range records {
				record_to_value[r] = bucket
			}

			// bookkeeping for info.db
			tb.update_int_info(k, bucket)
		}

		intCol.BucketEncoded = true
		// the column is high cardinality?
		if len(intCol.Bins) > CHUNK_SIZE/10 {
			intCol.BucketEncoded = false
			intCol.Bins = nil
			intCol.Values = make([]int64, count)
			for r, v := range record_to_value {
				intCol.Values[r] = v
			}
		}

		// Sort the int buckets before saving them, so we don't have to sort them after reading.
		sort.Sort(SortIntsByVal(intCol.Bins))

		col_fname := fmt.Sprintf("%s/int_%s.db", dirname, tb.get_string_for_key(k))

		var network bytes.Buffer // Stand-in for the network.

		// Create an encoder and send a value.
		enc := gob.NewEncoder(&network)
		err := enc.Encode(intCol)

		if err != nil {
			log.Fatal("encode:", err)
		}

		action := "SERIALIZED"
		if intCol.BucketEncoded {
			action = "BUCKETED  "
		}

		log.Println(action, "COLUMN BLOCK", col_fname, network.Len(), "BYTES", "( PER RECORD", network.Len()/len(tb.RecordList), ")")

		w, _ := os.Create(col_fname)

		network.WriteTo(w)
	}

}

func (tb *TableBlock) SaveSetsToColumns(dirname string, same_sets map[int16]ValueMap) {
	for k, v := range same_sets {
		col_name := tb.get_string_for_key(k)
		if col_name == "" {
			// TODO: validate what this means. I think it means reading 'null' values off disk
			// when pulling off incomplete records
			log.Println("CANT FIGURE OUT FIELD NAME FOR", k, "PROBABLY AN ERRONEOUS FIELD")
			continue
		}
		setCol := SavedSetColumn{}
		setCol.Name = col_name
		setCol.NameId = k
		setCol.DeltaEncodedIDs = DELTA_ENCODE_RECORD_IDS
		temp_block := newTableBlock()

		tb_col := tb.getColumnInfo(k)
		temp_col := temp_block.getColumnInfo(k)
		record_to_value := make(map[uint32][]int32)
		count := 0
		for bucket, records := range v {
			// migrating string definitions from column definitions
			str_val := tb_col.get_string_for_val(int32(bucket))
			str_id := temp_col.get_val_id(str_val)
			si := SavedSetBucket{Value: int32(str_id), Records: records}
			setCol.Bins = append(setCol.Bins, si)
			count += len(records)
			for _, r := range records {
				_, ok := record_to_value[r]
				if !ok {
					record_to_value[r] = make([]int32, 0)

				}

				record_to_value[r] = append(record_to_value[r], str_id)
			}
		}

		setCol.StringTable = make([]string, len(temp_col.StringTable))
		for str, id := range temp_col.StringTable {
			setCol.StringTable[id] = str
		}

		// the column is high cardinality?
		setCol.BucketEncoded = true
		if len(setCol.Bins) > CHUNK_SIZE/10 {
			setCol.BucketEncoded = false
			setCol.Bins = nil
			setCol.Values = make([][]int32, count)
			for k, v := range record_to_value {
				setCol.Values[k] = v
			}
		}

		col_fname := fmt.Sprintf("%s/set_%s.db", dirname, tb.get_string_for_key(k))

		var network bytes.Buffer // Stand-in for the network.

		// Create an encoder and send a value.
		enc := gob.NewEncoder(&network)
		err := enc.Encode(setCol)

		if err != nil {
			log.Fatal("encode:", err)
		}

		action := "SERIALIZED"
		if setCol.BucketEncoded {
			action = "BUCKETED  "
		}

		log.Println(action, "COLUMN BLOCK", col_fname, network.Len(), "BYTES", "( PER RECORD", network.Len()/len(tb.RecordList), ")")

		w, _ := os.Create(col_fname)
		network.WriteTo(w)

	}
}

func (tb *TableBlock) SaveStrsToColumns(dirname string, same_strs map[int16]ValueMap) {
	for k, v := range same_strs {
		col_name := tb.get_string_for_key(k)
		if col_name == "" {
			// TODO: validate what this means. I think it means reading 'null' values off disk
			// when pulling off incomplete records
			log.Println("CANT FIGURE OUT FIELD NAME FOR", k, "PROBABLY AN ERRONEOUS FIELD")
			continue
		}
		strCol := SavedStrColumn{}
		strCol.Name = col_name
		strCol.NameId = k
		strCol.DeltaEncodedIDs = DELTA_ENCODE_RECORD_IDS
		temp_block := newTableBlock()

		temp_col := temp_block.getColumnInfo(k)
		tb_col := tb.getColumnInfo(k)
		record_to_value := make(map[uint32]int32)
		count := 0
		for bucket, records := range v {

			// migrating string definitions from column definitions
			str_id := temp_col.get_val_id(tb_col.get_string_for_val(int32(bucket)))

			si := SavedStrBucket{Value: str_id, Records: records}
			strCol.Bins = append(strCol.Bins, si)
			count += len(records)
			for _, r := range records {
				record_to_value[r] = str_id
			}

			// also bookkeeping to be used later inside the block info.db, IMO
			tb.update_str_info(k, int(bucket), len(records))
		}

		strCol.BucketEncoded = true
		// the column is high cardinality?
		if len(strCol.Bins) > CHUNK_SIZE/10 {
			strCol.BucketEncoded = false
			strCol.Bins = nil
			strCol.Values = make([]int32, count)
			for k, v := range record_to_value {
				strCol.Values[k] = v
			}
		}

		tb.get_str_info(k).prune()

		strCol.StringTable = make([]string, len(temp_col.StringTable))
		for str, id := range temp_col.StringTable {
			strCol.StringTable[id] = str
		}

		col_fname := fmt.Sprintf("%s/str_%s.db", dirname, tb.get_string_for_key(k))

		var network bytes.Buffer // Stand-in for the network.

		// Create an encoder and send a value.
		enc := gob.NewEncoder(&network)
		err := enc.Encode(strCol)

		if err != nil {
			log.Fatal("encode:", err)
		}

		action := "SERIALIZED"
		if strCol.BucketEncoded {
			action = "BUCKETED  "
		}

		log.Println(action, "COLUMN BLOCK", col_fname, network.Len(), "BYTES", "( PER RECORD", network.Len()/len(tb.RecordList), ")")

		w, _ := os.Create(col_fname)
		network.WriteTo(w)

	}
}

func (tb *TableBlock) SaveInfoToColumns(dirname string) {
	records := tb.RecordList

	// Now to save block info...
	col_fname := fmt.Sprintf("%s/info.db", dirname)

	var network bytes.Buffer // Stand-in for the network.

	// Create an encoder and send a value.
	enc := gob.NewEncoder(&network)
	colInfo := SavedColumnInfo{NumRecords: int32(len(records)), IntInfo: INT_INFO_BLOCK[tb.Name], StrInfo: STR_INFO_BLOCK[tb.Name]}
	err := enc.Encode(colInfo)

	if err != nil {
		log.Fatal("encode:", err)
	}

	log.Println("SERIALIZED BLOCK INFO", col_fname, network.Len(), "BYTES", "( PER RECORD", network.Len()/len(records), ")")

	w, _ := os.Create(col_fname)
	network.WriteTo(w)
}

type SeparatedColumns struct {
	ints map[int16]ValueMap
	strs map[int16]ValueMap
	sets map[int16]ValueMap
}

func (tb *TableBlock) SeparateRecordsIntoColumns() SeparatedColumns {
	records := tb.RecordList

	// making a cross section of records that share values
	// goes from fieldname{} -> value{} -> record
	same_ints := make(map[int16]ValueMap)
	same_strs := make(map[int16]ValueMap)
	same_sets := make(map[int16]ValueMap)

	// parse record list and transfer book keeping data into the current
	// table block, as well as separate record values by column type
	for i, r := range records {
		for k, v := range r.Ints {
			if r.Populated[k] == INT_VAL {
				record_value(same_ints, int32(i), int16(k), int64(v))
			}
		}
		for k, v := range r.Strs {
			// transition key from the
			col := r.block.getColumnInfo(int16(k))
			new_col := tb.getColumnInfo(int16(k))

			v_name := col.get_string_for_val(int32(v))
			v_id := new_col.get_val_id(v_name)

			// record the transitioned key
			if r.Populated[k] == STR_VAL {
				record_value(same_strs, int32(i), int16(k), int64(v_id))
			}
		}
		for k, v := range r.SetMap {
			col := r.block.getColumnInfo(int16(k))
			new_col := tb.getColumnInfo(int16(k))
			if r.Populated[k] == SET_VAL {
				for _, iv := range v {
					v_name := col.get_string_for_val(int32(iv))
					v_id := new_col.get_val_id(v_name)
					record_value(same_sets, int32(i), int16(k), int64(v_id))
				}
			}
		}
	}

	if DELTA_ENCODE_RECORD_IDS {
		delta_encode(same_ints)
		delta_encode(same_strs)
		delta_encode(same_sets)
	}

	ret := SeparatedColumns{ints: same_ints, strs: same_strs, sets: same_sets}
	return ret

}

func (tb *TableBlock) SaveToColumns(filename string) {
	dirname := strings.Replace(filename, ".db", "", 1)

	// Important to set the BLOCK's dirName so we can keep track
	// of the various block infos
	tb.Name = dirname

	partialname := fmt.Sprintf("%s.partial", dirname)
	os.RemoveAll(partialname)
	oldblock := fmt.Sprintf("%s.old", dirname)

	separated_columns := tb.SeparateRecordsIntoColumns()

	tb.SaveIntsToColumns(partialname, separated_columns.ints)
	tb.SaveStrsToColumns(partialname, separated_columns.strs)
	tb.SaveSetsToColumns(partialname, separated_columns.sets)
	tb.SaveInfoToColumns(partialname)

	log.Println("FINISHED BLOCK", partialname, "RELINKING TO", dirname)

	// remove the old block
	os.RemoveAll(oldblock)
	err := os.Rename(dirname, oldblock)
	err = os.Rename(partialname, dirname)

	if err == nil {
		os.RemoveAll(oldblock)
	} else {
		log.Println("ERROR SAVING BLOCK", err)
	}
}

func (tb *TableBlock) unpackStrCol(dec *gob.Decoder, info SavedColumnInfo) {
	records := tb.RecordList

	into := &SavedStrColumn{}
	err := dec.Decode(into)

	col_name := tb.table.get_string_for_key(int(into.NameId))
	if col_name != into.Name {
		shouldbe := tb.table.get_key_id(into.Name)
		log.Println("WARNING: BLOCK", tb.Name, "HAS MISMATCHED COL INFO", into.Name, into.NameId, "IS", col_name, "BUT SHOULD BE", shouldbe, "SKIPPING!")
		return

	}
	string_lookup := make(map[int32]string)

	if err != nil {
		log.Println("DECODE COL ERR:", err)
	}

	col := tb.getColumnInfo(into.NameId)
	// unpack the string table
	for k, v := range into.StringTable {
		col.StringTable[v] = int32(k)
		string_lookup[int32(k)] = v
	}
	col.val_string_id_lookup = string_lookup

	var record *Record

	if into.BucketEncoded {
		for _, bucket := range into.Bins {

			prev := uint32(0)
			for _, r := range bucket.Records {
				val := string_lookup[bucket.Value]
				value_id := col.get_val_id(val)

				if into.DeltaEncodedIDs {
					r = prev + r
				}

				record = records[r]
				prev = r

				if int(into.NameId) >= len(record.Strs) {
					log.Println("FOUND A STRAY COLUMN...", into.Name, "RECORD LEN", len(record.Strs))
				} else {
					record.Strs[into.NameId] = StrField(value_id)
				}
				record.Populated[into.NameId] = STR_VAL
			}
		}
	} else {
		for r, v := range into.Values {
			records[r].Strs[into.NameId] = StrField(v)
			records[r].Populated[into.NameId] = STR_VAL
		}

	}

}

func (tb *TableBlock) unpackSetCol(dec *gob.Decoder, info SavedColumnInfo) {
	records := tb.RecordList

	into := &SavedSetColumn{}
	err := dec.Decode(into)
	if err != nil {
		log.Println("DECODE COL ERR:", err)
	}

	col_name := tb.table.get_string_for_key(int(into.NameId))
	if col_name != into.Name {
		shouldbe := tb.table.get_key_id(into.Name)
		log.Println("BLOCK", tb.Name, "HAS MISMATCHED COL INFO", into.Name, into.NameId, "IS", col_name, "BUT SHOULD BE", shouldbe)
	}

	string_lookup := make(map[int32]string)

	col := tb.getColumnInfo(into.NameId)
	// unpack the string table
	for k, v := range into.StringTable {
		col.StringTable[v] = int32(k)
		string_lookup[int32(k)] = v
	}
	col.val_string_id_lookup = string_lookup

	if into.BucketEncoded {
		for _, bucket := range into.Bins {
			// DONT FORGET TO DELTA UNENCODE THE RECORD VALUES
			prev := uint32(0)
			for _, r := range bucket.Records {
				if into.DeltaEncodedIDs {
					r = r + prev
				}

				cur_set, ok := records[r].SetMap[into.NameId]
				if !ok {
					cur_set = make(SetField, 0)
				}

				cur_set = append(cur_set, bucket.Value)
				records[r].SetMap[into.NameId] = cur_set

				records[r].Populated[into.NameId] = SET_VAL
				prev = r
			}

		}
	} else {
		log.Println("Uh-Oh, Trying to unencode Set column that's not bucket encoded")
		for r, v := range into.Values {
			cur_set, ok := records[r].SetMap[into.NameId]
			if !ok {
				cur_set = make(SetField, 0)
				records[r].SetMap[into.NameId] = cur_set
			}

			records[r].SetMap[into.NameId] = SetField(v)
			records[r].Populated[into.NameId] = SET_VAL
		}
	}
}

func (tb *TableBlock) unpackIntCol(dec *gob.Decoder, info SavedColumnInfo) {
	records := tb.RecordList

	into := &SavedIntColumn{}
	err := dec.Decode(into)
	if err != nil {
		log.Println("DECODE COL ERR:", err)
	}

	col_name := tb.table.get_string_for_key(int(into.NameId))
	if col_name != into.Name {
		shouldbe := tb.table.get_key_id(into.Name)
		log.Println("BLOCK", tb.Name, "HAS MISMATCHED COL INFO", into.Name, into.NameId, "IS", col_name, "BUT SHOULD BE", shouldbe)
	}

	if into.BucketEncoded {
		for _, bucket := range into.Bins {
			if *f_UPDATE_TABLE_INFO {
				tb.table.update_int_info(into.NameId, bucket.Value)
			}

			// DONT FORGET TO DELTA UNENCODE THE RECORD VALUES
			prev := uint32(0)
			for _, r := range bucket.Records {
				if into.DeltaEncodedIDs {
					r = r + prev
				}

				records[r].Ints[into.NameId] = IntField(bucket.Value)
				records[r].Populated[into.NameId] = INT_VAL
				prev = r
			}

		}
	} else {
		for r, v := range into.Values {
			if *f_UPDATE_TABLE_INFO {
				tb.table.update_int_info(into.NameId, v)
			}

			records[r].Ints[into.NameId] = IntField(v)
			records[r].Populated[into.NameId] = INT_VAL
		}
	}
}

func (tb *TableBlock) allocateRecords(loadSpec *LoadSpec, info SavedColumnInfo, load_records bool) RecordList {
	t := tb.table

	var r *Record

	var records RecordList
	var alloced []Record
	var bigIntArr IntArr
	var bigStrArr StrArr
	var bigPopArr []int8
	max_key_id := 0
	var has_sets = false
	var has_strs = false
	var has_ints = false
	for _, v := range t.KeyTable {
		if max_key_id <= int(v) {
			max_key_id = int(v) + 1
		}
	}

	// determine if we need to allocate the different field containers inside
	// each record
	if loadSpec != nil && load_records == false {
		for field_name, _ := range loadSpec.columns {
			v := t.get_key_id(field_name)

			switch t.KeyTypes[v] {
			case INT_VAL:
				has_ints = true
			case SET_VAL:
				has_sets = true
			case STR_VAL:
				has_strs = true
			default:
				log.Fatal("MISSING KEY TYPE FOR COL", v)
			}
		}
	} else {
		has_sets = true
		has_ints = true
		has_strs = true
	}

	if loadSpec != nil || load_records {
		mstart := time.Now()
		records = make(RecordList, info.NumRecords)
		alloced = make([]Record, info.NumRecords)
		if has_ints {
			bigIntArr = make(IntArr, max_key_id*int(info.NumRecords))
		}
		if has_strs {
			bigStrArr = make(StrArr, max_key_id*int(info.NumRecords))
		}
		bigPopArr = make([]int8, max_key_id*int(info.NumRecords))
		mend := time.Now()

		if DEBUG_TIMING {
			log.Println("MALLOCED RECORDS", info.NumRecords, "TOOK", mend.Sub(mstart))
		}

		start := time.Now()
		for i := range records {
			r = &alloced[i]
			if has_ints {
				r.Ints = bigIntArr[i*max_key_id : (i+1)*max_key_id]
			}

			if has_strs {
				r.Strs = bigStrArr[i*max_key_id : (i+1)*max_key_id]
			}

			// TODO: move this allocation next to the allocations above
			if has_sets {
				r.SetMap = make(SetMap)
			}

			r.Populated = bigPopArr[i*max_key_id : (i+1)*max_key_id]

			r.block = tb
			records[i] = r
		}
		end := time.Now()

		if DEBUG_TIMING {
			log.Println("INITIALIZED RECORDS", info.NumRecords, "TOOK", end.Sub(start))
		}
	}

	tb.RecordList = records[:]
	return records[:]

}
