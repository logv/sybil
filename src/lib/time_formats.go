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

func GetTimeFormat(timeFmt string) string {
	constFmt := strings.ToLower(timeFmt)
	timeFormat, ok := FORMATS[constFmt]
	if ok {
		Debug("USING TIME FORMAT", timeFormat, "FOR", timeFmt)
		return timeFormat
	}

	Debug("USING TIME FORMAT", timeFormat)
	return timeFormat
}
