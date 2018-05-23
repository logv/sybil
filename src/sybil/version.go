package sybil

var VERSION_STRING = "0.5.0"

func GetVersionInfo() map[string]interface{} {
	versionInfo := make(map[string]interface{})

	versionInfo["version"] = VERSION_STRING
	versionInfo["field_separator"] = true
	versionInfo["log_hist"] = true
	versionInfo["query_cache"] = true
	versionInfo["dist_queries"] = true

	return versionInfo

}
