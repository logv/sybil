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

var SINGLE_EVENT_DURATION = int64(30) // i think this means 30 seconds
var BLOCKS_BEFORE_GC = 8

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
		ss.SessionDelta.addValue(int64(records[0].Timestamp - ss.LastSessionEnd))
	}

	for _, r := range records {
		ss.Calendar.AddActivity(int(r.Timestamp))
	}

	if len(records) == 1 {
		ss.NumBounces.addValue(int64(1))
		return
	}

	last_index := len(records) - 1
	delta := records[last_index].Timestamp - records[0].Timestamp
	ss.SessionDuration.addValue(int64(delta))
	ss.LastSessionEnd = records[last_index].Timestamp

}

func (ss *SessionStats) PrintStats(key string) {
	duration := int(ss.SessionDuration.Avg / ss.NumSessions.Avg)
	fmt.Printf("%s:\n", key)
	fmt.Printf("  %d sessions\n", ss.NumSessions.Sum())
	fmt.Printf("  total events: %d\n", ss.NumEvents.Sum())

	if ss.NumBounces.Count > 0 {
		fmt.Printf("  total bounces: %d\n", ss.NumBounces.Count)
		bounce_rate := ss.NumBounces.Sum() * 1000 / ss.NumSessions.Sum()
		fmt.Printf("  bounce rate: %v%%\n", bounce_rate/10.0)
	}

	fmt.Printf("  avg events per session: %0.2f\n", float64(ss.NumEvents.Avg))
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
	prev_time := 0

	session_cutoff := *FLAGS.SESSION_CUTOFF * 60
	sessions := make([]RecordList, 0)
	if len(as.Records) <= 0 {
		as.Records = nil
		return sessions
	}

	var path_key bytes.Buffer
	var path_length = *FLAGS.PATH_LENGTH
	current_session := make(RecordList, 0)

	var avg_delta = 0.0
	var num_delta = 0.0
	var prev_deltas = make([]float64, 0)
	for _, r := range as.Records {
		time_val := int(r.Timestamp)

		if r.Path != "" {
			path_val := r.Path

			for i := 1; i < path_length; i++ {
				as.Path[i-1] = as.Path[i]
				path_key.WriteString(as.Path[i])
				path_key.WriteString(GROUP_DELIMITER)
			}

			as.Path[path_length-1] = path_val

			path_key.WriteString(r.Path)

			if as.PathLength < path_length {
				as.PathLength++
			} else {
				as.PathStats[path_key.String()]++
			}

			path_key.Reset()
		}

		if prev_time > 0 && time_val-prev_time > session_cutoff {
			sessions = append(sessions, current_session)

			current_session = make(RecordList, 0)
			current_session = append(current_session, r.CopyRecord())

			avg_delta = 0
			num_delta = 0
			prev_deltas = make([]float64, 0)

		} else {

			if prev_time > 0 {
				delta := float64(time_val - prev_time)
				prev_deltas = append(prev_deltas, delta)
				num_delta += 1
				div_delta := avg_delta
				if div_delta == 0 {
					div_delta = 1
				}

				avg_delta = avg_delta + (delta/div_delta)/num_delta
			}

			current_session = append(current_session, r.CopyRecord())
		}
		prev_time = time_val
	}

	if timestamp-prev_time > session_cutoff {
		sessions = append(sessions, current_session)

		current_session = nil
	}

	as.Records = current_session

	return sessions
}

func (sl *SessionList) AddRecord(group_key string, r *Record) {
	session, ok := sl.List[group_key]
	if !ok {
		session = &ActiveSession{}
		session.Records = make(RecordList, 0)
		session.Path = make([]string, *FLAGS.PATH_LENGTH)
		session.PathStats = make(map[string]int)
		session.Stats = NewSessionStats()
		sl.List[group_key] = session
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
	var path_uniques map[string]int = make(map[string]int)
	var path_counts map[string]int = make(map[string]int)

	sl := ss.Sessions

	if sl.JoinTable != nil {
		groups = strings.Split(*FLAGS.JOIN_GROUP, *FLAGS.FIELD_SEPARATOR)
	}

	for join_key, as := range sl.List {
		var group_key = ""
		join_key = strings.TrimSpace(join_key)

		if sl.JoinTable != nil {
			r := sl.JoinTable.GetRecordById(join_key)
			if r != nil {
				for _, g := range groups {
					g_id := sl.JoinTable.get_key_id(g)
					switch r.Populated[g_id] {
					case INT_VAL:
						group_key = strconv.FormatInt(int64(r.Ints[g_id]), 10)
					case STR_VAL:
						col := r.block.GetColumnInfo(g_id)
						group_key = col.get_string_for_val(int32(r.Strs[g_id]))
					}

				}
			}
		}

		if DEBUG_RECORD_CONSISTENCY {
			if group_key == "" {
				Debug("COULDNT FIND JOIN RECORD FOR", join_key)
			}
		}

		stats, ok := sl.Results[group_key]
		if !ok {
			stats = NewSessionStats()
			sl.Results[group_key] = stats
		}

		for k, v := range as.PathStats {
			path_counts[k] += v
			path_uniques[k] += 1
		}

		stats.CombineStats(as.Stats)
		duration := as.Stats.Calendar.Max - as.Stats.Calendar.Min

		retention := duration / int64(time.Hour.Seconds()*24)
		stats.Retention.addValue(int64(retention))

	}

	ss.Sessions.PathUniques = make(map[string]int)
	ss.Sessions.PathCounts = make(map[string]int)
	for key, count := range path_counts {
		ss.Sessions.PathCounts[key] = count
		ss.Sessions.PathUniques[key] = path_uniques[key]
	}

}

func (ss *SessionSpec) PrintResults() {
	Debug("SESSION STATS")
	Debug("UNIQUE SESSION IDS", len(ss.Sessions.List))

	Debug("SESSIONS", ss.Count)
	if len(ss.Sessions.List) > 0 {
		Debug("AVERAGE EVENTS PER SESSIONS", ss.Count/len(ss.Sessions.List))
	}

	if *FLAGS.PATH_KEY != "" {
		if *FLAGS.JSON {
			ret := make(map[string]interface{})
			ret["uniques"] = ss.Sessions.PathUniques
			ret["counts"] = ss.Sessions.PathCounts
			printJson(ret)
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
		prev_session, ok := ss.Sessions.List[key]
		if !ok {
			ss.Sessions.List[key] = as
		} else {
			prev_session.CombineSession(as)
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

		session_col := *FLAGS.SESSION_COL
		var group_key = bytes.NewBufferString("")

		cols := strings.Split(session_col, *FLAGS.FIELD_SEPARATOR)
		for _, col := range cols {
			field_id := r.block.get_key_id(col)
			switch r.Populated[field_id] {
			case INT_VAL:
				group_key.WriteString(strconv.FormatInt(int64(r.Ints[field_id]), 10))

			case STR_VAL:
				field_col := r.block.GetColumnInfo(field_id)
				group_key.WriteString(field_col.get_string_for_val(int32(r.Strs[field_id])))

			case _NO_VAL:
				Debug("MISSING EVENT KEY!")

			}

			group_key.WriteString(GROUP_DELIMITER)

		}

		sessionSpec.Sessions.AddRecord(group_key.String(), r)

		records[i] = nil
	}

}

type SortBlocksByTime []*TableBlock

func (a SortBlocksByTime) Len() int      { return len(a) }
func (a SortBlocksByTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SortBlocksByTime) Less(i, j int) bool {
	time_col := *FLAGS.TIME_COL
	return a[i].Info.IntInfoMap[time_col].Min < a[j].Info.IntInfoMap[time_col].Min
}

type SortBlocksByEndTime []*TableBlock

func (a SortBlocksByEndTime) Len() int      { return len(a) }
func (a SortBlocksByEndTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SortBlocksByEndTime) Less(i, j int) bool {
	time_col := *FLAGS.TIME_COL
	return a[i].Info.IntInfoMap[time_col].Max < a[j].Info.IntInfoMap[time_col].Max
}

func LoadAndSessionize(tables []*Table, querySpec *QuerySpec, sessionSpec *SessionSpec) int {

	blocks := make(SortBlocksByTime, 0)

	for _, t := range tables {
		for _, b := range t.BlockList {
			block := t.LoadBlockFromDir(b.Name, nil, false)
			if block != nil {
				if block.Info.IntInfoMap[*FLAGS.TIME_COL] != nil {
					block.table = t
					blocks = append(blocks, block)

				}
			}

		}
	}

	sort.Sort(SortBlocksByTime(blocks))
	Debug("SORTED BLOCKS", len(blocks))

	masterSession := NewSessionSpec()
	// Setup the join table for the session spec
	if *FLAGS.JOIN_TABLE != "" {
		start := time.Now()
		Debug("LOADING JOIN TABLE", *FLAGS.JOIN_TABLE)
		jt := GetTable(*FLAGS.JOIN_TABLE)
		masterSession.Sessions.JoinTable = jt

		joinLoadSpec := jt.NewLoadSpec()
		joinLoadSpec.LoadAllColumns = true

		DELETE_BLOCKS_AFTER_QUERY = false
		FLAGS.READ_INGESTION_LOG = &TRUE
		jt.LoadRecords(&joinLoadSpec)
		end := time.Now()

		Debug("LOADING JOIN TABLE TOOK", end.Sub(start))

		jt.BuildJoinMap()

	}

	max_time := int64(0)
	count := 0

	var wg sync.WaitGroup

	result_lock := sync.Mutex{}
	count_lock := sync.Mutex{}

	filterSpec := FilterSpec{Int: *FLAGS.INT_FILTERS, Str: *FLAGS.STR_FILTERS, Set: *FLAGS.SET_FILTERS}

	for i, b := range blocks {

		min_time := b.Info.IntInfoMap[*FLAGS.TIME_COL].Min

		max_time = b.Info.IntInfoMap[*FLAGS.TIME_COL].Max
		this_block := b
		block_index := i
		wg.Add(1)
		go func() {

			//			Debug("LOADING BLOCK", this_block.Name, min_time)
			if *FLAGS.DEBUG {
				fmt.Fprint(os.Stderr, ".")
			}
			blockQuery := CopyQuerySpec(querySpec)
			blockSession := NewSessionSpec()
			loadSpec := this_block.table.NewLoadSpec()
			if *FLAGS.PATH_KEY != "" {
				loadSpec.Str(*FLAGS.PATH_KEY)
			}

			cols := strings.Split(*FLAGS.SESSION_COL, *FLAGS.FIELD_SEPARATOR)
			for _, col := range cols {
				loadSpec.Str(col)
			}
			loadSpec.Int(*FLAGS.TIME_COL)

			filters := BuildFilters(this_block.table, &loadSpec, filterSpec)
			blockQuery.Filters = filters

			block := this_block.table.LoadBlockFromDir(this_block.Name, &loadSpec, false)
			if block != nil {

				SessionizeRecords(blockQuery, &blockSession, &block.RecordList)
				count_lock.Lock()
				count += len(block.RecordList)
				count_lock.Unlock()
			}

			result_lock.Lock()
			masterSession.CombineSessions(&blockSession)
			this_block.RecordList = nil
			block.RecordList = nil
			delete(block.table.BlockList, block.Name)

			result_lock.Unlock()

			wg.Done()
		}()

		if block_index%BLOCKS_BEFORE_GC == 0 && block_index > 0 {
			wg.Wait()

			if *FLAGS.DEBUG {
				fmt.Fprintf(os.Stderr, "+")
			}

			go func() {
				old_percent := debug.SetGCPercent(100)
				debug.SetGCPercent(old_percent)

			}()

			result_lock.Lock()
			masterSession.Sessions.NoMoreRecordsBefore(int(min_time))
			masterSession.ExpireRecords()

			result_lock.Unlock()

		}

	}

	wg.Wait()

	fmt.Fprintf(os.Stderr, "+")
	session_cutoff := *FLAGS.SESSION_CUTOFF * 60
	masterSession.Sessions.NoMoreRecordsBefore(int(max_time) + 2*session_cutoff)
	masterSession.ExpireRecords()
	fmt.Fprintf(os.Stderr, "\n")
	Debug("INSPECTED", count, "RECORDS")

	// Kick off the final grouping, aggregations and joining of sessions
	masterSession.Finalize()
	masterSession.PrintResults()

	return count

}
