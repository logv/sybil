package sybil

import "fmt"

import "os"
import "sort"
import "strings"
import "strconv"
import "sync"
import "time"
import "bytes"
import "runtime/debug"

// TODO:
// * add first pass at filters
// * add event level aggregations for a session

// GOALS:
// Query support: "time spent on site", "retention", "common paths"
// Do not use too much memory, be able to run live queries

// FILTERING
// session contains an event (or not) with specific criterion for the event
// session contains an event (or not) followed by specific event

// Filters are defined via descriptions of specific records & what to pull out of it
// event1: description
// event2: description
// filter: event1 follows event2
// filter: event2 does not exist
// filter: event1 does not follow event2

// SESSION AGGREGATIONS
// x length of sessions
// x actions per session
// x frequency of sessions (by calendar day)
// x common session patterns (pathing)
// * number of actions per fixed time period

var SingleEventDuration = int64(30) // i think this means 30 seconds
var BlocksBeforeGc = 8

type SessionSpec struct {
	ExpireAfter int // Seconds to expire a session after not seeing any new events

	Sessions SessionList
	Count    int
}

func NewSessionSpec() SessionSpec {
	ss := SessionSpec{}

	ss.Sessions.List = make(Sessions)
	ss.Sessions.Results = make(map[string]*SessionStats)

	return ss
}

func (ss *SessionSpec) ExpireRecords() {
	ss.Count += ss.Sessions.ExpireRecords()
}

type Sessions map[string]*ActiveSession
type SessionList struct {
	List Sessions

	JoinTable *Table
	Results   map[string]*SessionStats

	PathCounts  map[string]int
	PathUniques map[string]int

	Expiration     int
	LastExpiration int
}

func (sl *SessionList) ExpireRecords() int {
	if sl.LastExpiration == sl.Expiration {
		return 0
	}

	count := 0
	m := &sync.Mutex{}
	var wg sync.WaitGroup
	for _, as := range sl.List {
		wg.Add(1)
		bs := as
		go func() {
			sort.Sort(SortRecordsByTime{bs.Records})

			sessions := bs.ExpireRecords(sl.Expiration)

			for _, session := range sessions {
				bs.Stats.SummarizeSession(session)
			}

			m.Lock()
			count += len(sessions)
			m.Unlock()

			wg.Done()
		}()

	}

	wg.Wait()

	sl.LastExpiration = sl.Expiration

	return count
}

type ActiveSession struct {
	Records RecordList
	Stats   *SessionStats

	Path       []string
	PathKey    bytes.Buffer
	PathLength int
	PathStats  map[string]int
}

type SessionStats struct {
	NumEvents       BasicHist
	NumBounces      BasicHist
	NumSessions     BasicHist
	SessionDuration BasicHist
	Retention       BasicHist
	Calendar        *Calendar

	SessionDelta BasicHist

	LastSessionEnd int64
}

func NewSessionStats() *SessionStats {
	ss := SessionStats{}
	ss.Calendar = NewCalendar()
	return &ss
}

func (ss *SessionStats) CombineStats(stats *SessionStats) {
	ss.NumEvents.Combine(&stats.NumEvents)
	ss.NumBounces.Combine(&stats.NumBounces)
	ss.NumSessions.Combine(&stats.NumSessions)
	ss.SessionDuration.Combine(&stats.SessionDuration)
	ss.SessionDelta.Combine(&stats.SessionDelta)

	ss.Calendar.CombineCalendar(stats.Calendar)
}

func (ss *SessionStats) SummarizeSession(records RecordList) {
	if len(records) == 0 {
		return
	}

	ss.NumEvents.addValue(int64(len(records)))
	ss.NumSessions.addValue(int64(1))

	if ss.LastSessionEnd > 0 {
		ss.SessionDelta.addValue(records[0].Timestamp - ss.LastSessionEnd)
	}

	for _, r := range records {
		ss.Calendar.AddActivity(int(r.Timestamp))
	}

	if len(records) == 1 {
		ss.NumBounces.addValue(int64(1))
		return
	}

	lastIndex := len(records) - 1
	delta := records[lastIndex].Timestamp - records[0].Timestamp
	ss.SessionDuration.addValue(delta)
	ss.LastSessionEnd = records[lastIndex].Timestamp

}

func (ss *SessionStats) PrintStats(key string) {
	duration := int(ss.SessionDuration.Avg / ss.NumSessions.Avg)
	fmt.Printf("%s:\n", key)
	fmt.Printf("  %d sessions\n", ss.NumSessions.Sum())
	fmt.Printf("  total events: %d\n", ss.NumEvents.Sum())

	if ss.NumBounces.Count > 0 {
		fmt.Printf("  total bounces: %d\n", ss.NumBounces.Count)
		bounceRate := ss.NumBounces.Sum() * 1000 / ss.NumSessions.Sum()
		fmt.Printf("  bounce rate: %v%%\n", bounceRate/10.0)
	}

	fmt.Printf("  avg events per session: %0.2f\n", ss.NumEvents.Avg)
	if duration > 0 {
		fmt.Printf("  avg duration: %d minutes\n", duration/60)
	}

	fmt.Printf("  avg retention: %d days\n", int(ss.Retention.Avg))
}

func (as *ActiveSession) AddRecord(r *Record) {
	// TODO: Figure out where to put the record using sort indeces and slice insertion
	as.Records = append(as.Records, r)
}

func (as *ActiveSession) IsExpired() bool {

	return false
}

func (as *ActiveSession) ExpireRecords(timestamp int) []RecordList {
	prevTime := 0

	sessionCutoff := *FLAGS.SessionCutoff * 60
	sessions := make([]RecordList, 0)
	if len(as.Records) <= 0 {
		as.Records = nil
		return sessions
	}

	var pathKey bytes.Buffer
	var pathLength = *FLAGS.PathLength
	currentSession := make(RecordList, 0)

	var avgDelta = 0.0
	var numDelta = 0.0
	var prevDeltas = make([]float64, 0)
	for _, r := range as.Records {
		timeVal := int(r.Timestamp)

		if r.Path != "" {
			pathVal := r.Path

			for i := 1; i < pathLength; i++ {
				as.Path[i-1] = as.Path[i]
				pathKey.WriteString(as.Path[i])
				pathKey.WriteString(GroupDelimiter)
			}

			as.Path[pathLength-1] = pathVal

			pathKey.WriteString(r.Path)

			if as.PathLength < pathLength {
				as.PathLength++
			} else {
				as.PathStats[pathKey.String()]++
			}

			pathKey.Reset()
		}

		if prevTime > 0 && timeVal-prevTime > sessionCutoff {
			sessions = append(sessions, currentSession)

			currentSession = make(RecordList, 0)
			currentSession = append(currentSession, r.CopyRecord())

			avgDelta = 0
			numDelta = 0
			prevDeltas = make([]float64, 0)

		} else {

			if prevTime > 0 {
				delta := float64(timeVal - prevTime)
				prevDeltas = append(prevDeltas, delta)
				numDelta += 1
				divDelta := avgDelta
				if divDelta == 0 {
					divDelta = 1
				}

				avgDelta = avgDelta + (delta/divDelta)/numDelta
			}

			currentSession = append(currentSession, r.CopyRecord())
		}
		prevTime = timeVal
	}

	if timestamp-prevTime > sessionCutoff {
		sessions = append(sessions, currentSession)

		currentSession = nil
	}

	as.Records = currentSession

	return sessions
}

func (sl *SessionList) AddRecord(groupKey string, r *Record) {
	session, ok := sl.List[groupKey]
	if !ok {
		session = &ActiveSession{}
		session.Records = make(RecordList, 0)
		session.Path = make([]string, *FLAGS.PathLength)
		session.PathStats = make(map[string]int)
		session.Stats = NewSessionStats()
		sl.List[groupKey] = session
	}

	session.AddRecord(r)
}

func (as *ActiveSession) CombineSession(session *ActiveSession) {
	for _, r := range session.Records {
		as.AddRecord(r)
	}

	for k, v := range session.PathStats {
		as.PathStats[k] += v
	}
}

func (as *SessionList) NoMoreRecordsBefore(timestamp int) {
	as.Expiration = timestamp
}

func (ss *SessionSpec) Finalize() {

	var groups []string
	var pathUniques map[string]int = make(map[string]int)
	var pathCounts map[string]int = make(map[string]int)

	sl := ss.Sessions

	if sl.JoinTable != nil {
		groups = strings.Split(*FLAGS.JoinGroup, *FLAGS.FieldSeparator)
	}

	for joinKey, as := range sl.List {
		var groupKey = ""
		joinKey = strings.TrimSpace(joinKey)

		if sl.JoinTable != nil {
			r := sl.JoinTable.GetRecordByID(joinKey)
			if r != nil {
				for _, g := range groups {
					gID := sl.JoinTable.getKeyID(g)
					switch r.Populated[gID] {
					case IntVal:
						groupKey = strconv.FormatInt(int64(r.Ints[gID]), 10)
					case StrVal:
						col := r.block.GetColumnInfo(gID)
						groupKey = col.getStringForVal(int32(r.Strs[gID]))
					}

				}
			}
		}

		if DebugRecordConsistency {
			if groupKey == "" {
				Debug("COULDNT FIND JOIN RECORD FOR", joinKey)
			}
		}

		stats, ok := sl.Results[groupKey]
		if !ok {
			stats = NewSessionStats()
			sl.Results[groupKey] = stats
		}

		for k, v := range as.PathStats {
			pathCounts[k] += v
			pathUniques[k] += 1
		}

		stats.CombineStats(as.Stats)
		duration := as.Stats.Calendar.Max - as.Stats.Calendar.Min

		retention := duration / int64(time.Hour.Seconds()*24)
		stats.Retention.addValue(retention)

	}

	ss.Sessions.PathUniques = make(map[string]int)
	ss.Sessions.PathCounts = make(map[string]int)
	for key, count := range pathCounts {
		ss.Sessions.PathCounts[key] = count
		ss.Sessions.PathUniques[key] = pathUniques[key]
	}

}

func (ss *SessionSpec) PrintResults() {
	Debug("SESSION STATS")
	Debug("UNIQUE SESSION IDS", len(ss.Sessions.List))

	Debug("SESSIONS", ss.Count)
	if len(ss.Sessions.List) > 0 {
		Debug("AVERAGE EVENTS PER SESSIONS", ss.Count/len(ss.Sessions.List))
	}

	if *FLAGS.PathKey != "" {
		if *FLAGS.JSON {
			ret := make(map[string]interface{})
			ret["uniques"] = ss.Sessions.PathUniques
			ret["counts"] = ss.Sessions.PathCounts
			printJSON(ret)
			fmt.Println("")
		} else {
			Debug("PATHS", len(ss.Sessions.PathCounts))
		}
	} else {
		for key, s := range ss.Sessions.Results {
			s.PrintStats(key)
		}
	}

}

func (ss *SessionSpec) CombineSessions(sessionspec *SessionSpec) {
	for key, as := range sessionspec.Sessions.List {
		prevSession, ok := ss.Sessions.List[key]
		if !ok {
			ss.Sessions.List[key] = as
		} else {
			prevSession.CombineSession(as)
		}
	}
}

func SessionizeRecords(querySpec *QuerySpec, sessionSpec *SessionSpec, recordsptr *RecordList) {
	records := *recordsptr
	for i := 0; i < len(records); i++ {
		r := records[i]

		add := true
		// FILTERING
		for j := 0; j < len(querySpec.Filters); j++ {
			// returns True if the record matches!
			ret := querySpec.Filters[j].Filter(r) != true
			if ret {
				add = false
				break
			}
		}

		if !add {
			continue
		}

		sessionCol := *FLAGS.SessionCol
		var groupKey = bytes.NewBufferString("")

		cols := strings.Split(sessionCol, *FLAGS.FieldSeparator)
		for _, col := range cols {
			fieldID := r.block.getKeyID(col)
			switch r.Populated[fieldID] {
			case IntVal:
				groupKey.WriteString(strconv.FormatInt(int64(r.Ints[fieldID]), 10))

			case StrVal:
				fieldCol := r.block.GetColumnInfo(fieldID)
				groupKey.WriteString(fieldCol.getStringForVal(int32(r.Strs[fieldID])))

			case _NoVal:
				Debug("MISSING EVENT KEY!")

			}

			groupKey.WriteString(GroupDelimiter)

		}

		sessionSpec.Sessions.AddRecord(groupKey.String(), r)

		records[i] = nil
	}

}

type SortBlocksByTime []*TableBlock

func (a SortBlocksByTime) Len() int      { return len(a) }
func (a SortBlocksByTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SortBlocksByTime) Less(i, j int) bool {
	timeCol := *FLAGS.TimeCol
	return a[i].Info.IntInfoMap[timeCol].Min < a[j].Info.IntInfoMap[timeCol].Min
}

type SortBlocksByEndTime []*TableBlock

func (a SortBlocksByEndTime) Len() int      { return len(a) }
func (a SortBlocksByEndTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SortBlocksByEndTime) Less(i, j int) bool {
	timeCol := *FLAGS.TimeCol
	return a[i].Info.IntInfoMap[timeCol].Max < a[j].Info.IntInfoMap[timeCol].Max
}

func LoadAndSessionize(tables []*Table, querySpec *QuerySpec, sessionSpec *SessionSpec) int {

	blocks := make(SortBlocksByTime, 0)

	for _, t := range tables {
		for _, b := range t.BlockList {
			block := t.LoadBlockFromDir(b.Name, nil, false)
			if block != nil {
				if block.Info.IntInfoMap[*FLAGS.TimeCol] != nil {
					block.table = t
					blocks = append(blocks, block)

				}
			}

		}
	}

	sort.Sort(blocks)
	Debug("SORTED BLOCKS", len(blocks))

	masterSession := NewSessionSpec()
	// Setup the join table for the session spec
	if *FLAGS.JoinTable != "" {
		start := time.Now()
		Debug("LOADING JOIN TABLE", *FLAGS.JoinTable)
		jt := GetTable(*FLAGS.JoinTable)
		masterSession.Sessions.JoinTable = jt

		joinLoadSpec := jt.NewLoadSpec()
		joinLoadSpec.LoadAllColumns = true

		DeleteBlocksAfterQuery = false
		FLAGS.ReadIngestionLog = &TRUE
		jt.LoadRecords(&joinLoadSpec)
		end := time.Now()

		Debug("LOADING JOIN TABLE TOOK", end.Sub(start))

		jt.BuildJoinMap()

	}

	maxTime := int64(0)
	count := 0

	var wg sync.WaitGroup

	resultLock := sync.Mutex{}
	countLock := sync.Mutex{}

	filterSpec := FilterSpec{Int: *FLAGS.IntFilters, Str: *FLAGS.StrFilters, Set: *FLAGS.SetFilters}

	for i, b := range blocks {

		minTime := b.Info.IntInfoMap[*FLAGS.TimeCol].Min

		maxTime = b.Info.IntInfoMap[*FLAGS.TimeCol].Max
		thisBlock := b
		blockIndex := i
		wg.Add(1)
		go func() {

			//			Debug("LOADING BLOCK", thisBlock.Name, minTime)
			if *FLAGS.Debug {
				fmt.Fprint(os.Stderr, ".")
			}
			blockQuery := CopyQuerySpec(querySpec)
			blockSession := NewSessionSpec()
			loadSpec := thisBlock.table.NewLoadSpec()
			if *FLAGS.PathKey != "" {
				loadSpec.Str(*FLAGS.PathKey)
			}

			cols := strings.Split(*FLAGS.SessionCol, *FLAGS.FieldSeparator)
			for _, col := range cols {
				loadSpec.Str(col)
			}
			loadSpec.Int(*FLAGS.TimeCol)

			filters := BuildFilters(thisBlock.table, &loadSpec, filterSpec)
			blockQuery.Filters = filters

			block := thisBlock.table.LoadBlockFromDir(thisBlock.Name, &loadSpec, false)
			if block != nil {

				SessionizeRecords(blockQuery, &blockSession, &block.RecordList)
				countLock.Lock()
				count += len(block.RecordList)
				countLock.Unlock()
			}

			resultLock.Lock()
			masterSession.CombineSessions(&blockSession)
			thisBlock.RecordList = nil
			block.RecordList = nil
			delete(block.table.BlockList, block.Name)

			resultLock.Unlock()

			wg.Done()
		}()

		if blockIndex%BlocksBeforeGc == 0 && blockIndex > 0 {
			wg.Wait()

			if *FLAGS.Debug {
				fmt.Fprintf(os.Stderr, "+")
			}

			go func() {
				oldPercent := debug.SetGCPercent(100)
				debug.SetGCPercent(oldPercent)

			}()

			resultLock.Lock()
			masterSession.Sessions.NoMoreRecordsBefore(int(minTime))
			masterSession.ExpireRecords()

			resultLock.Unlock()

		}

	}

	wg.Wait()

	fmt.Fprintf(os.Stderr, "+")
	sessionCutoff := *FLAGS.SessionCutoff * 60
	masterSession.Sessions.NoMoreRecordsBefore(int(maxTime) + 2*sessionCutoff)
	masterSession.ExpireRecords()
	fmt.Fprintf(os.Stderr, "\n")
	Debug("INSPECTED", count, "RECORDS")

	// Kick off the final grouping, aggregations and joining of sessions
	masterSession.Finalize()
	masterSession.PrintResults()

	return count

}
