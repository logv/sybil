// +build !profile

package edb

var f_PROFILE *bool
var PROFILER_ENABLED bool

type NoProfile struct{}

func (p NoProfile) Start() ProfilerStart {
	return NoProfile{}
}
func (p NoProfile) Stop() {

}

var RUN_PROFILER = func() ProfilerStop {
	return NoProfile{}
}
