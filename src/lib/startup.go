package sybil

import "github.com/logv/sybil/src/lib/common"

func Startup() {
	registerTypesForQueryCache()
	common.SetDefaults()
}
