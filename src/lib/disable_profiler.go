// +build !profile

package sybil

var ProfilerEnabled bool
var PROFILE *bool = &ProfilerEnabled

type NoProfile struct{}

func (p NoProfile) Start() ProfilerStart {
	return NoProfile{}
}
func (p NoProfile) Stop() {

}

var StopProfiler = func() {
}
var RunProfiler = func() ProfilerStop {
	return NoProfile{}
}
