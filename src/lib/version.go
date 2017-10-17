package sybil

var VERSION_STRING = "0.2.0"

func GetVersionInfo() map[string]interface{} {
	versionInfo := make(map[string]interface{})

	versionInfo["version"] = VERSION_STRING
	versionInfo["field_separator"] = true
	versionInfo["log_hist"] = true
	versionInfo["query_cache"] = true

	if EnableHdr {
		versionInfo["hdr_hist"] = true

	}

	return versionInfo

}
