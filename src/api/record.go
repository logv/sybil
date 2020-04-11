package api

import "encoding/json"

// {{{ SybilRecord methods

func (r *SybilRecord) Int(field string, value int) *SybilRecord {
	r.Ints[field] = value
	return r
}
func (r *SybilRecord) Str(field string, value string) *SybilRecord {
	r.Strs[field] = value
	return r
}
func (r *SybilRecord) Set(field string, value []string) *SybilRecord {
	r.Sets[field] = value
	return r
}

func (r *SybilRecord) JSON() []byte {
	record := make(map[string]interface{})

	for k, v := range r.Strs {
		record[k] = v
	}
	for k, v := range r.Ints {
		record[k] = v
	}
	for k, v := range r.Sets {
		record[k] = v
	}

	bytes, err := json.Marshal(record)
	if err == nil {
		return bytes
	} else {
		Error("COULDNT CONVERT SYBIL RECORD TO JSON", r)
	}

	return nil
}

// }}}
