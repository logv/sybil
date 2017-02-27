package sybil


import "strings"
import "time"

var FORMATS = map[string]string{
	"ansic":       time.ANSIC,
	"unixdate":    time.UnixDate,
	"rubydate":    time.RubyDate,
	"rfc822":      time.RFC822,
	"rfc822z":     time.RFC822Z,
	"rfc850":      time.RFC850,
	"rfc1123":     time.RFC1123,
	"rfc1123z":    time.RFC1123Z,
	"rfc3339":     time.RFC3339,
	"rfc3339nano": time.RFC3339Nano,
	"kitchen":     time.Kitchen,
	"stamp":       time.Stamp,
	"stampmilli":  time.StampMilli,
	"stampmicro":  time.StampMicro,
	"stampnano":   time.StampNano,
}

func GetTimeFormat(time_fmt string) string {
	const_fmt := strings.ToLower(time_fmt)
	time_format, ok := FORMATS[const_fmt]
	if ok {
		Debug("USING TIME FORMAT", time_format, "FOR", time_fmt)
		return time_format
	}

	Debug("USING TIME FORMAT", time_format)
	return time_format
}
