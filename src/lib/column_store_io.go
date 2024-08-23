package sybil

import "fmt"
import "bytes"

import "os"
import "errors"
import "encoding/gob"
import "runtime/debug"
import "time"
import "regexp"

type ValueMap map[int64][]uint32

// After testing various cardinalities for timestamps, the optimal seems to be
// about 5000 (or even less) unique values.
// TODO: determine optimal for different sized integers other than timestamps
var CARDINALITY_THRESHOLD = 5000
var DEBUG_RECORD_CONSISTENCY = false

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
		if len(col) <= CARDINALITY_THRESHOLD {
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

func (tb *TableBlock) GetColumnInfo(name_id int16) *TableColumn {
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
			Debug("CANT FIGURE OUT FIELD NAME FOR", k, "SOMETHING IS PROBABLY AWRY")
			continue
		}
		intCol := NewSavedIntColumn()

		intCol.Name = col_name
		intCol.DeltaEncodedIDs = true

		max_r := 0
		record_to_value := make(map[uint32]int64)
		for bucket, records := range v {
			si := SavedIntBucket{Value: bucket, Records: records}
			intCol.Bins = append(intCol.Bins, si)
			for _, r := range records {
				record_to_value[r] = bucket
				if int(r) >= max_r {
					max_r = int(r) + 1
				}
			}

			// bookkeeping for info.db
			tb.update_int_info(k, bucket)
			tb.table.update_int_info(k, bucket)
		}

		intCol.BucketEncoded = true
		// the column is high cardinality?
		if len(intCol.Bins) > CARDINALITY_THRESHOLD {
			intCol.BucketEncoded = false
			intCol.Bins = nil
			intCol.Values = make([]int64, max_r)
			intCol.ValueEncoded = true

			for r, val := range record_to_value {
				intCol.Values[r] = val
			}

			prev := int64(0)
			for r, val := range intCol.Values {
				intCol.Values[r] = val - prev
				prev = val
			}
		}

		var network bytes.Buffer

		col_type := "int"
		col_fname := fmt.Sprintf("%s/%s_%s.db", dirname, col_type, tb.get_string_for_key(k))
		// Create an encoder and send a value.
		enc := gob.NewEncoder(&network)
		err := enc.Encode(intCol)
		if err != nil {
			Error("encode:", err)
		}

		action := "SERIALIZED"
		if intCol.BucketEncoded {
			action = "BUCKETED  "
		}

		Debug(action, "COLUMN BLOCK", col_fname, network.Len(), "BYTES", "( PER RECORD", network.Len()/len(tb.RecordList), ")")

		w, _ := os.Create(col_fname)

		network.WriteTo(w)
	}

}

func (tb *TableBlock) SaveFloatsToColumns(dirname string, floats map[int16][]FloatField) {
	// now make the dir and shoot each blob out into a separate file
	// SAVED TO A SINGLE BLOCK ON DISK, NOW TO SAVE IT OUT TO SEPARATE VALUES
	os.MkdirAll(dirname, 0777)
	for k, v := range floats {
		col_name := tb.get_string_for_key(k)
		if col_name == "" {
			Debug("CANT FIGURE OUT FIELD NAME FOR", k, "SOMETHING IS PROBABLY AWRY")
			continue
		}
		floatCol := NewSavedFloatColumn()

		floatCol.Name = col_name
		floatCol.Values = make([]float64, len(v))
		for i, vv := range v {
			floatCol.Values[i] = float64(vv)

			// bookkeeping for info.db
			tb.update_int_info(k, int64(vv+1))
			tb.table.update_int_info(k, int64(vv+1))
			tb.update_int_info(k, int64(vv))
			tb.table.update_int_info(k, int64(vv))
		}

		var network bytes.Buffer
		enc := gob.NewEncoder(&network)
		err := enc.Encode(floatCol)
		if err != nil {
			Error("encode:", err)
		}

		col_type := "float"
		col_fname := fmt.Sprintf("%s/%s_%s.db", dirname, col_type, tb.get_string_for_key(k))

		Debug("SERIALIZED COLUMN BLOCK", col_fname, network.Len(), "BYTES", "( PER RECORD", network.Len()/len(tb.RecordList), ")")

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
			Debug("CANT FIGURE OUT FIELD NAME FOR", k, "PROBABLY AN ERRONEOUS FIELD")
			continue
		}
		setCol := SavedSetColumn{}
		setCol.Name = col_name
		setCol.DeltaEncodedIDs = true
		temp_block := newTableBlock()

		tb_col := tb.GetColumnInfo(k)
		temp_col := temp_block.GetColumnInfo(k)
		record_to_value := make(map[uint32][]int32)
		max_r := 0
		for bucket, records := range v {
			// migrating string definitions from column definitions
			str_val := tb_col.get_string_for_val(int32(bucket))
			str_id := temp_col.get_val_id(str_val)
			si := SavedSetBucket{Value: int32(str_id), Records: records}
			setCol.Bins = append(setCol.Bins, si)
			for _, r := range records {
				_, ok := record_to_value[r]
				if int(r) >= max_r {
					max_r = int(r) + 1
				}

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
		if len(setCol.Bins) > CARDINALITY_THRESHOLD {
			setCol.BucketEncoded = false
			setCol.Bins = nil
			setCol.Values = make([][]int32, max_r)
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
			Error("encode:", err)
		}

		action := "SERIALIZED"
		if setCol.BucketEncoded {
			action = "BUCKETED  "
		}

		Debug(action, "COLUMN BLOCK", col_fname, network.Len(), "BYTES", "( PER RECORD", network.Len()/len(tb.RecordList), ")")

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
			Debug("CANT FIGURE OUT FIELD NAME FOR", k, "PROBABLY AN ERRONEOUS FIELD")
			continue
		}
		strCol := NewSavedStrColumn()
		strCol.Name = col_name
		strCol.DeltaEncodedIDs = true
		temp_block := newTableBlock()

		temp_col := temp_block.GetColumnInfo(k)
		tb_col := tb.GetColumnInfo(k)
		record_to_value := make(map[uint32]int32)
		max_r := 0
		for bucket, records := range v {

			// migrating string definitions from column definitions
			str_id := temp_col.get_val_id(tb_col.get_string_for_val(int32(bucket)))

			si := SavedStrBucket{Value: str_id, Records: records}
			strCol.Bins = append(strCol.Bins, si)
			for _, r := range records {
				record_to_value[r] = str_id
				if r >= uint32(max_r) {
					max_r = int(r) + 1
				}
			}

			// also bookkeeping to be used later inside the block info.db, IMO
			tb.update_str_info(k, int(bucket), len(records))
		}

		strCol.BucketEncoded = true
		// the column is high cardinality?
		if len(strCol.Bins) > CARDINALITY_THRESHOLD {
			strCol.BucketEncoded = false
			strCol.Bins = nil
			strCol.Values = make([]int32, max_r)
			for k, v := range record_to_value {
				strCol.Values[k] = v
			}
		}

		for _, bucket := range strCol.Bins {
			first_val := bucket.Records[0]
			if first_val > 1000 && DEBUG_RECORD_CONSISTENCY {
				Warn(k, bucket.Value, "FIRST RECORD IS", first_val)
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
			Error("encode:", err)
		}

		action := "SERIALIZED"
		if strCol.BucketEncoded {
			action = "BUCKETED  "
		}

		Debug(action, "COLUMN BLOCK", col_fname, network.Len(), "BYTES", "( PER RECORD", network.Len()/len(tb.RecordList), ")")

		w, _ := os.Create(col_fname)
		network.WriteTo(w)

	}
}

type SavedIntInfo map[string]*IntInfo
type SavedStrInfo map[string]*StrInfo

func (tb *TableBlock) SaveInfoToColumns(dirname string) {
	records := tb.RecordList

	// Now to save block info...
	col_fname := fmt.Sprintf("%s/info.db", dirname)

	var network bytes.Buffer // Stand-in for the network.

	// Create an encoder and send a value.
	enc := gob.NewEncoder(&network)

	savedIntInfo := SavedIntInfo{}
	savedStrInfo := SavedStrInfo{}
	if tb.Info != nil {
		if tb.Info.IntInfoMap != nil {
			savedIntInfo = tb.Info.IntInfoMap
		}
		if tb.Info.StrInfoMap != nil {
			savedStrInfo = tb.Info.StrInfoMap
		}
	}

	for k, v := range tb.IntInfo {
		name := tb.get_string_for_key(k)
		savedIntInfo[name] = v
	}

	for k, v := range tb.StrInfo {
		name := tb.get_string_for_key(k)
		savedStrInfo[name] = v
	}

	colInfo := SavedColumnInfo{NumRecords: int32(len(records)), IntInfoMap: savedIntInfo, StrInfoMap: savedStrInfo}
	err := enc.Encode(colInfo)

	if err != nil {
		Error("encode:", err)
	}

	length := len(records)
	if length == 0 {
		length = 1
	}

	if DEBUG_TIMING {
		Debug("SERIALIZED BLOCK INFO", col_fname, network.Len(), "BYTES", "( PER RECORD", network.Len()/length, ")")
	}

	w, _ := os.Create(col_fname)
	network.WriteTo(w)
}

type SeparatedColumns struct {
	ints   map[int16]ValueMap
	strs   map[int16]ValueMap
	sets   map[int16]ValueMap
	floats map[int16][]FloatField
}

func (tb *TableBlock) SeparateRecordsIntoColumns() SeparatedColumns {
	records := tb.RecordList

	// making a cross section of records that share values
	// goes from fieldname{} -> value{} -> record
	same_ints := make(map[int16]ValueMap)
	same_strs := make(map[int16]ValueMap)
	same_sets := make(map[int16]ValueMap)
	same_floats := make(map[int16][]FloatField)

	// parse record list and transfer book keeping data into the current
	// table block, as well as separate record values by column type
	for i, r := range records {
		for k, v := range r.Ints {
			if r.Populated[k] == INT_VAL {
				record_value(same_ints, int32(i), int16(k), int64(v))
			}
		}
		for k, v := range r.Floats {
			if r.Populated[k] == FLOAT_VAL {
				ik := int16(k)
				_, ok := same_floats[ik]
				if !ok {
					same_floats[ik] = make([]FloatField, 0)
				}

				same_floats[ik] = append(same_floats[ik], v)
			}
		}
		for k, v := range r.Strs {

			// record the transitioned key
			if r.Populated[k] == STR_VAL {
				// transition key from the
				col := r.block.GetColumnInfo(int16(k))
				new_col := tb.GetColumnInfo(int16(k))

				v_name := col.get_string_for_val(int32(v))
				v_id := new_col.get_val_id(v_name)

				record_value(same_strs, int32(i), int16(k), int64(v_id))
			}
		}
		for k, v := range r.SetMap {
			if r.Populated[k] == SET_VAL {
				col := r.block.GetColumnInfo(int16(k))
				new_col := tb.GetColumnInfo(int16(k))
				for _, iv := range v {
					v_name := col.get_string_for_val(int32(iv))
					v_id := new_col.get_val_id(v_name)
					record_value(same_sets, int32(i), int16(k), int64(v_id))
				}
			}
		}
	}

	delta_encode(same_ints)
	delta_encode(same_strs)
	delta_encode(same_sets)

	ret := SeparatedColumns{ints: same_ints, strs: same_strs, sets: same_sets, floats: same_floats}
	return ret

}

func (tb *TableBlock) SaveToColumns(filename string) bool {
	dirname := filename

	// Important to set the BLOCK's dirName so we can keep track
	// of the various block infos
	tb.Name = dirname

	defer tb.table.ReleaseBlockLock(filename)
	if tb.table.GrabBlockLock(filename) == false {
		Debug("Can't grab lock to save block", filename)
		return false
	}

	partialname := fmt.Sprintf("%s.partial", dirname)
	oldblock := fmt.Sprintf("%s.old", dirname)

	start := time.Now()
	old_percent := debug.SetGCPercent(-1)
	separated_columns := tb.SeparateRecordsIntoColumns()
	end := time.Now()
	Debug("COLLATING BLOCKS TOOK", end.Sub(start))

	tb.SaveIntsToColumns(partialname, separated_columns.ints)
	tb.SaveStrsToColumns(partialname, separated_columns.strs)
	tb.SaveSetsToColumns(partialname, separated_columns.sets)
	tb.SaveFloatsToColumns(partialname, separated_columns.floats)
	tb.SaveInfoToColumns(partialname)

	end = time.Now()
	Debug("FINISHED BLOCK", partialname, "RELINKING TO", dirname, "TOOK", end.Sub(start))

	debug.SetGCPercent(old_percent)

	// TODO: Add a stronger consistency check here
	// For now, we load info.db and check NumRecords inside it to prevent
	// catastrophics, but we could load everything potentially
	start = time.Now()
	nb := tb.table.LoadBlockFromDir(partialname, nil, false)
	end = time.Now()

	// TODO:
	if nb == nil || nb.Info.NumRecords != int32(len(tb.RecordList)) {
		Error("COULDNT VALIDATE CONSISTENCY FOR RECENTLY SAVED BLOCK!", filename)
	}

	if DEBUG_RECORD_CONSISTENCY {
		nb = tb.table.LoadBlockFromDir(partialname, nil, true)
		if nb == nil || len(nb.RecordList) != len(tb.RecordList) {
			Error("DEEP VALIDATION OF BLOCK FAILED CONSISTENCY CHECK!", filename)
		}
	}

	Debug("VALIDATED NEW BLOCK HAS", nb.Info.NumRecords, "RECORDS, TOOK", end.Sub(start))

	os.RemoveAll(oldblock)
	err := RenameAndMod(dirname, oldblock)
	if err != nil {
		Error("ERROR RENAMING BLOCK", dirname, oldblock, err)
	}
	err = RenameAndMod(partialname, dirname)
	if err != nil {
		Error("ERROR RENAMING PARTIAL", partialname, dirname, err)
	}

	if err == nil {
		os.RemoveAll(oldblock)
	} else {
		Error("ERROR SAVING BLOCK", partialname, dirname, err)
	}

	Debug("RELEASING BLOCK", tb.Name)
	return true

}

func (tb *TableBlock) unpackStrCol(dec FileDecoder, info SavedColumnInfo) error {
	records := tb.RecordList[:]

	into := &SavedStrColumn{}
	err := dec.Decode(into)
	if err != nil {
		Debug("DECODE COL ERR:", err)
		return nil
	}

	string_lookup := make([]string, info.NumRecords)
	key_table_len := len(records[0].Strs)
	col_id := tb.table.get_key_id(into.Name)
	num_records := uint32(tb.Info.NumRecords)

	if int(col_id) >= key_table_len {
		Debug("IGNORING STR COLUMN", into.Name, "SINCE ITS NOT IN KEY TABLE IN BLOCK", tb.Name)
		return nil
	}

	col := tb.GetColumnInfo(col_id)
	// unpack the string table

	// Run our replacements!
	str_replace, ok := OPTS.STR_REPLACEMENTS[into.Name]
	bucket_replace := make(map[int32]int32)
	var re *regexp.Regexp
	if ok {
		re, err = regexp.Compile(str_replace.Pattern)
	}

	if uint32(len(into.StringTable)) > num_records {
		return errors.New("BLOCK SIZE CHANGED DURING QUERY")
	}

	for k, v := range into.StringTable {
		var nv = v
		if re != nil {
			nv = re.ReplaceAllString(v, str_replace.Replace)
		}

		existing_key, exists := col.StringTable[nv]

		v = nv

		if exists {
			bucket_replace[int32(k)] = existing_key
		} else {
			bucket_replace[int32(k)] = int32(k)
			col.StringTable[v] = int32(k)
		}

		string_lookup[int32(k)] = v
	}

	col.val_string_id_lookup = string_lookup

	var record *Record
	var r uint32

	if into.BucketEncoded {
		prev := uint32(0)
		did := into.DeltaEncodedIDs

		for _, bucket := range into.Bins {
			prev = 0
			value := bucket.Value
			new_value, should_replace := bucket_replace[value]
			if should_replace {
				value = new_value
			}

			cast_value := StrField(new_value)
			for _, r = range bucket.Records {

				if did {
					r = prev + r
				}

				if r >= num_records {
					return errors.New("BLOCK SIZE CHANGED DURING QUERY")
				}

				prev = r
				record = records[r]

				if DEBUG_RECORD_CONSISTENCY {
					if record.Populated[col_id] != _NO_VAL {
						Error("OVERWRITING RECORD VALUE", record, into.Name, col_id, bucket.Value)
					}
				}

				records[r].Populated[col_id] = STR_VAL
				records[r].Strs[col_id] = cast_value

			}
		}

	} else {
		if uint32(len(into.Values)) > num_records {
			return errors.New("BLOCK SIZE CHANGED DURING QUERY")
		}

		for r, v := range into.Values {
			new_value, should_replace := bucket_replace[v]
			if should_replace {
				v = new_value
			}

			records[r].Strs[col_id] = StrField(v)
			records[r].Populated[col_id] = STR_VAL
		}

	}

	return nil
}

func (tb *TableBlock) unpackSetCol(dec FileDecoder, info SavedColumnInfo) error {
	records := tb.RecordList

	saved_col := NewSavedSetColumn()
	into := &saved_col
	err := dec.Decode(into)
	if err != nil {
		Debug("DECODE COL ERR:", err)
	}

	key_table_len := len(tb.table.KeyTable)
	col_id := tb.table.get_key_id(into.Name)
	string_lookup := make(map[int32]string)

	if int(col_id) >= key_table_len {
		Debug("IGNORING SET COLUMN", into.Name, "SINCE ITS NOT IN KEY TABLE IN BLOCK", tb.Name)
		return nil
	}

	col := tb.GetColumnInfo(col_id)
	// unpack the string table
	for k, v := range into.StringTable {
		col.StringTable[v] = int32(k)
		string_lookup[int32(k)] = v
	}

	tr_string_lookup := make([]string, len(string_lookup))
	for k, v := range string_lookup {
		tr_string_lookup[k] = v
	}

	col.val_string_id_lookup = tr_string_lookup

	num_records := uint32(tb.Info.NumRecords)
	if into.BucketEncoded {
		for _, bucket := range into.Bins {
			// DONT FORGET TO DELTA UNENCODE THE RECORD VALUES
			prev := uint32(0)
			for _, r := range bucket.Records {
				if into.DeltaEncodedIDs {
					r = r + prev
				}

				if r >= num_records {
					return errors.New("BLOCK SIZE CHANGED DURING QUERY")
				}

				cur_set, ok := records[r].SetMap[col_id]
				if !ok {
					cur_set = make(SetField, 0)
				}

				cur_set = append(cur_set, bucket.Value)
				records[r].SetMap[col_id] = cur_set

				records[r].Populated[col_id] = SET_VAL
				prev = r
			}

		}
	} else {
		if uint32(len(into.Values)) > num_records {
			return errors.New("BLOCK SIZE CHANGED DURING QUERY")
		}
		for r, v := range into.Values {
			cur_set, ok := records[r].SetMap[col_id]
			if !ok {
				cur_set = make(SetField, 0)
				records[r].SetMap[col_id] = cur_set
			}

			records[r].SetMap[col_id] = SetField(v)
			records[r].Populated[col_id] = SET_VAL
		}
	}

	return nil
}

func (tb *TableBlock) unpackIntCol(dec FileDecoder, info SavedColumnInfo) error {
	records := tb.RecordList[:]

	into := &SavedIntColumn{}
	err := dec.Decode(into)
	if err != nil {
		Debug("DECODE COL ERR:", err)
	}

	key_table_len := len(records[0].Ints)
	col_id := tb.table.get_key_id(into.Name)
	if int(col_id) >= key_table_len {
		Debug("IGNORING INT COLUMN", into.Name, "SINCE ITS NOT IN KEY TABLE IN BLOCK", tb.Name)
		return nil
	}

	is_time_col := false
	if FLAGS.TIME_COL != "" {
		is_time_col = into.Name == FLAGS.TIME_COL
	}

	num_records := uint32(tb.Info.NumRecords)

	if into.BucketEncoded {
		for _, bucket := range into.Bins {
			if FLAGS.UPDATE_TABLE_INFO {
				tb.update_int_info(col_id, bucket.Value)
				tb.table.update_int_info(col_id, bucket.Value)
			}

			// DONT FORGET TO DELTA UNENCODE THE RECORD VALUES
			prev := uint32(0)
			for _, r := range bucket.Records {
				if into.DeltaEncodedIDs {
					r = r + prev
				}

				if DEBUG_RECORD_CONSISTENCY {
					if records[r].Populated[col_id] != _NO_VAL {
						Error("OVERWRITING RECORD VALUE", records[r], into.Name, col_id, bucket.Value)
					}
				}

				if r >= num_records {
					return errors.New("BLOCK SIZE CHANGED DURING QUERY")
				}

				records[r].Ints[col_id] = IntField(bucket.Value)
				records[r].Populated[col_id] = INT_VAL
				prev = r

				if is_time_col {
					records[r].Timestamp = bucket.Value
				}

			}

		}
	} else {

		prev := int64(0)
		if uint32(len(into.Values)) > num_records {
			return errors.New("BLOCK SIZE CHANGED DURING QUERY")
		}

		for r, v := range into.Values {
			if FLAGS.UPDATE_TABLE_INFO {
				tb.update_int_info(col_id, v)
				tb.table.update_int_info(col_id, v)
			}

			if into.ValueEncoded {
				v = v + prev
			}

			records[r].Ints[col_id] = IntField(v)
			records[r].Populated[col_id] = INT_VAL

			if is_time_col {
				records[r].Timestamp = v
			}

			if into.ValueEncoded {
				prev = v
			}

		}
	}

	return nil
}

func (tb *TableBlock) unpackFloatCol(dec FileDecoder, info SavedColumnInfo) error {
	records := tb.RecordList[:]

	into := &SavedFloatColumn{}
	err := dec.Decode(into)
	if err != nil {
		Debug("DECODE COL ERR:", err)
	}

	key_table_len := len(records[0].Floats)
	col_id := tb.table.get_key_id(into.Name)
	if int(col_id) >= key_table_len {
		Debug("IGNORING FLOAT COLUMN", into.Name, "SINCE ITS NOT IN KEY TABLE IN BLOCK", tb.Name)
		return nil
	}

	for r, v := range into.Values {
		records[r].Floats[col_id] = FloatField(v)
		records[r].Populated[col_id] = FLOAT_VAL

		if FLAGS.UPDATE_TABLE_INFO {
			tb.update_int_info(col_id, int64(v+1))
			tb.table.update_int_info(col_id, int64(v+1))
			tb.update_int_info(col_id, int64(v))
			tb.table.update_int_info(col_id, int64(v))
		}
	}

	return nil
}
