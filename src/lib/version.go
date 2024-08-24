package sybil

var VERSION_STRING = "0.5.2"

func GetVersionInfo() map[string]interface{} {
	version_info := make(map[string]interface{})

	version_info["version"] = VERSION_STRING
	version_info["field_separator"] = true
	version_info["log_hist"] = true
	version_info["query_cache"] = true
	version_info["dist_queries"] = true
	version_info["tdigest"] = ENABLE_TDIGEST
	version_info["short_key_table"] = true
	version_info["sort_asc"] = true

	return version_info

}
