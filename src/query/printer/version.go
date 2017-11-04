package sybil

import hists "github.com/logv/sybil/src/query/hists"

var VERSION_STRING = "0.2.0"

func GetVersionInfo() map[string]interface{} {
	version_info := make(map[string]interface{})

	version_info["version"] = VERSION_STRING
	version_info["field_separator"] = true
	version_info["log_hist"] = true
	version_info["query_cache"] = true

	if hists.ENABLE_HDR {
		version_info["hdr_hist"] = true

	}

	return version_info

}
