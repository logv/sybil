//+build profile

package sybil

import "github.com/pkg/profile"

var ProfilerEnabled = true
var PROFILE ProfilerStart

type PkgProfile struct {
}

func (p PkgProfile) Start() ProfilerStart {
	if *FLAGS.ProfileMem {
		PROFILE = profile.Start(profile.MemProfile, profile.ProfilePath("."))
	} else {
		PROFILE = profile.Start(profile.CPUProfile, profile.ProfilePath("."))
	}
	return PROFILE
}
func (p PkgProfile) Stop() {
	p.Stop()
}

func StopProfiler() {
	PROFILE.Stop()
}

var RunProfiler = func() ProfilerStop {
	Debug("RUNNING ENABLED PROFILER")
	return PkgProfile{}
}
