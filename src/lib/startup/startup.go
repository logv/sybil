package sybil

import "github.com/logv/sybil/src/lib/config"
import . "github.com/logv/sybil/src/lib/column_store"

func Startup() {
	RegisterTypesForQueryCache()
	config.SetDefaults()
}
