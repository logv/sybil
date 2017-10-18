//+build profile

package config

import (
	"github.com/logv/sybil/src/lib/config"
	"github.com/pkg/profile"
)

var PROFILER_ENABLED = true
var PROFILE ProfilerStart

type PkgProfile struct {
}

func (p PkgProfile) Start() ProfilerStart {
	if *config.FLAGS.PROFILE_MEM {
		PROFILE = profile.Start(profile.MemProfile, profile.ProfilePath("."))
	} else {
		PROFILE = profile.Start(profile.CPUProfile, profile.ProfilePath("."))
	}
	return PROFILE
}
func (p PkgProfile) Stop() {
	p.Stop()
}

func STOP_PROFILER() {
	PROFILE.Stop()
}

var RUN_PROFILER = func() ProfilerStop {
	common.Debug("RUNNING ENABLED PROFILER")
	return PkgProfile{}
}
