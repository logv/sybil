//+build profile

package sybil

import "github.com/pkg/profile"
import "log"

import "flag"

var f_PROFILE = flag.Bool("profile", false, "turn profiling on?")
var f_PROFILE_MEM = flag.Bool("mem", false, "turn memory profiling on")

var PROFILER_ENABLED = true
var PROFILE ProfilerStart

type PkgProfile struct {
}

func (p PkgProfile) Start() ProfilerStart {
	if *f_PROFILE_MEM {
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
