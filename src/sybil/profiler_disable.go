// +build !profile

package sybil

var PROFILER_ENABLED bool
var PROFILE = &PROFILER_ENABLED // nolint

type NoProfile struct{}

func (p NoProfile) Start() ProfilerStart {
	return NoProfile{}
}
func (p NoProfile) Stop() {

}

var STOP_PROFILER = func() {
}
var RUN_PROFILER = func() ProfilerStop {
	return NoProfile{}
}
