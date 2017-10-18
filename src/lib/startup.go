package sybil

import "github.com/logv/sybil/src/lib/config"

func Startup() {
	registerTypesForQueryCache()
	config.SetDefaults()
}
