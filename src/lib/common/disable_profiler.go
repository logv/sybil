// +build !profile

package common

var (
	PROFILER_ENABLED bool
	PROFILE          *bool = &PROFILER_ENABLED
)

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
