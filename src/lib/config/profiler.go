package config

type ProfilerStart interface {
	Stop()
}

type ProfilerStop interface {
	Start() ProfilerStart
}
