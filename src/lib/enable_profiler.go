//+build profile

package sybil

import "github.com/pkg/profile"
import "log"

var PROFILER_ENABLED = true
var PROFILE ProfilerStart

type PkgProfile struct {
}

func (p PkgProfile) Start() ProfilerStart {
	if *FLAGS.PROFILE_MEM {
		PROFILE = profile.Start(profile.MemProfile, profile.ProfilePath("."))
	} else {
		PROFILE = profile.Start(profile.CPUProfile, profile.ProfilePath("."))
	}
	return PROFILE
}
func (p PkgProfile) Stop() {
	p.Stop()
}

var RUN_PROFILER = func() ProfilerStop {
	log.Println("RUNNING ENABLED PROFILER")
	return PkgProfile{}
}
