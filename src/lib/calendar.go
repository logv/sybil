package sybil

import "math"
import "time"

type Activity struct {
	Count int
}

type ActivityMap map[int]Activity

// Trying out a calendar with stats by day, week and month
type Calendar struct {
	Daily   ActivityMap
	Weekly  ActivityMap
	Monthly ActivityMap

	Min int
	Max int
}

func NewCalendar() *Calendar {
	c := Calendar{}

	c.Daily = make(ActivityMap)
	c.Weekly = make(ActivityMap)
	c.Monthly = make(ActivityMap)
	c.Min = math.MaxInt64
	c.Max = 0

	return &c
}

func punch_calendar(am *ActivityMap, timestamp int) {
	is, ok := (*am)[timestamp]

	if !ok {
		is = Activity{}
		(*am)[timestamp] = is
	}
}

func copy_calendar(am1, am2 ActivityMap) {
	for k, v := range am2 {
		is, ok := am1[k]
		if ok {
			is.Count += v.Count
		} else {
			am1[k] = v
		}
	}
}

func (c *Calendar) AddActivity(timestamp int) {
	if *FLAGS.RETENTION != false {
		punch_calendar(&c.Daily, timestamp/(int(time.Hour.Seconds())*24))
		punch_calendar(&c.Weekly, timestamp/(int(time.Hour.Seconds())*24*7))
		punch_calendar(&c.Monthly, timestamp/(int(time.Hour.Seconds())*24*7*30))
	}

	c.Min = int(math.Min(float64(timestamp), float64(c.Min)))
	c.Max = int(math.Max(float64(timestamp), float64(c.Max)))
}

func (c *Calendar) CombineCalendar(cc *Calendar) {
	copy_calendar(c.Daily, cc.Daily)
	copy_calendar(c.Weekly, cc.Weekly)
	copy_calendar(c.Monthly, cc.Monthly)

	c.Min = int(math.Min(float64(cc.Min), float64(c.Min)))
	c.Max = int(math.Max(float64(cc.Max), float64(c.Max)))
}
