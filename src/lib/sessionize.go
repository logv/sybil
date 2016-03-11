package sybil

import "fmt"
import "log"
import "os"
import "sort"
import "strings"
import "strconv"
import "sync"
import "time"

// DECIDE:
// should metadata be emitted into an event stream or should it be pulled via a join key?
// for now... i guess metadata should be made via join key?

// NEXT:
// * to do this, need to have our scripts generate multiple record types?
// * do a join using the group key for the session

// AFTER:
// * add first pass at filters
// * add event level aggregations

// GOALS:
// Query support: "time spent on site", "number of sessions", ("retention"?)
// Do not use too much memory, be able to run live queries

// FILTERING
// session contains an event (with specific criterion for the event)
// session contains an event followed by specific event
// session does not contain event

// Filters are defined via descriptions of specific records & what to pull out of it
// event1: description
// event2: description
// filter: event1 follows event2
// filter: event2 does not exist
// filter: event1 does not follow event2

// AGGREGATING
// Aggregation based metrics in timeline:
// Aggregation of timelines:
// * length of sessions
// * actions per session
// * frequency of sessions (by calendar day)
// * number of actions per fixed time period
// * common session patterns

var SINGLE_EVENT_DURATION = int64(30)

type SessionSpec struct {
	ExpireAfter int // Seconds to expire a session after not seeing any new events

	block *TableBlock

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

type Sessions map[int32]*ActiveSession
type SessionList struct {
	List Sessions

	JoinTable *Table
	Results   map[string]*SessionStats

	block *TableBlock

	Expiration     int
	LastExpiration int
}

func (sl *SessionList) ExpireRecords() int {
	if sl.LastExpiration == sl.Expiration {
		return 0
	}

	t := GetTable(*FLAGS.TABLE)

	time_col := *FLAGS.TIME_COL
	col_id := t.get_key_id(time_col)
	count := 0

	for _, as := range sl.List {
		sort.Sort(SortRecordsByTime{as.Records, col_id})

		sessions := as.ExpireRecords(sl.Expiration)

		for _, session := range sessions {
			as.Stats.SummarizeSession(session)
		}

		count += len(sessions)

	}

	sl.LastExpiration = sl.Expiration

	return count
}

type ActiveSession struct {
	Records RecordList
	Stats   SessionStats
}

type SessionStats struct {
	NumEvents       Hist
	NumSessions     Hist
	SessionDuration Hist

	SessionDelta Hist

	LastSessionEnd int64

	Calendar map[int]bool
}

func (ss *SessionStats) CombineStats(stats *SessionStats) {
	ss.NumEvents.Combine(&stats.NumEvents)
	ss.NumSessions.Combine(&stats.NumSessions)
	ss.SessionDuration.Combine(&stats.SessionDuration)
	ss.SessionDelta.Combine(&stats.SessionDelta)

	// TODO: combine the Calendar too
}

func (ss *SessionStats) SummarizeSession(records RecordList) {
	if len(records) == 0 {
		return
	}

	ss.NumEvents.addValue(len(records))
	ss.NumSessions.addValue(1)

	if ss.LastSessionEnd > 0 {
		ss.SessionDelta.addValue(int(records[0].Timestamp - ss.LastSessionEnd))
	}

	if len(records) == 1 {
		ss.SessionDuration.addValue(int(SINGLE_EVENT_DURATION))
		return
	}

	last_index := len(records) - 1
	delta := records[last_index].Timestamp - records[0].Timestamp
	ss.SessionDuration.addValue(int(delta))
	ss.LastSessionEnd = records[last_index].Timestamp
}

func (ss *SessionStats) PrintStats(key string) {
	fmt.Printf("%s:\n", key)
	fmt.Printf("  %d sessions\n", ss.NumSessions.Sum())
	fmt.Printf("  total events: %d\n", ss.NumEvents.Sum())
	fmt.Printf("  avg event per session: %d\n", int(ss.NumEvents.Avg))
	fmt.Printf("  avg duration: %d\n", int(ss.SessionDuration.Avg/ss.NumSessions.Avg))
}

func (as *ActiveSession) AddRecord(r *Record) {
	// TODO: Figure out where to put the record using sort indeces and slice insertion
	as.Records = append(as.Records, r)
}

func (as *ActiveSession) IsExpired() bool {

	return false
}

var SESSION_CUTOFF = 60 * 60 * 24 // 1 day

func (as *ActiveSession) ExpireRecords(timestamp int) []RecordList {
	prev_time := 0
	//	slice_start := 0
	last_slice := as.Records[:]
	slice_start := 0
	sessions := make([]RecordList, 0)

	if len(as.Records) <= 0 {
		return sessions
	}

	time_field := as.Records[0].block.get_key_id(*FLAGS.TIME_COL)
	time_val := 0
	ok := false

	for i, r := range as.Records {
		ok = r.Populated[time_field] == INT_VAL
		time_val = int(r.Ints[time_field])

		if ok && prev_time > 0 && time_val-prev_time > SESSION_CUTOFF {
			last_slice = as.Records[i:]
			current_slice := as.Records[slice_start:i]
			sessions = append(sessions, current_slice)

			slice_start = i
		}
		prev_time = time_val

	}

	if timestamp-prev_time > SESSION_CUTOFF {
		sessions = append(sessions, last_slice)
		last_slice = as.Records[0:0]
	}

	as.Records = last_slice

	return sessions
}

func (sl *SessionList) AddRecord(group_key int32, r *Record) {
	session, ok := sl.List[group_key]
	if !ok {
		session = &ActiveSession{}
		session.Records = make(RecordList, 0)
		sl.List[group_key] = session
	}

	session.AddRecord(r)
}

func (as *ActiveSession) CombineSession(session *ActiveSession) {
	for _, r := range session.Records {
		as.AddRecord(r)
	}
}

func (as *SessionList) NoMoreRecordsBefore(timestamp int) {
	as.Expiration = timestamp
}

func (ss *SessionSpec) Finalize() {

	var groups []string

	sl := ss.Sessions

	if sl.JoinTable != nil {
		groups = strings.Split(*FLAGS.JOIN_GROUP, ",")
	}

	join_id := sl.block.get_key_id(*FLAGS.JOIN_KEY)
	join_col := sl.block.GetColumnInfo(join_id)

	for key, as := range sl.List {

		// TODO: determine if this is an int or string
		join_key := join_col.get_string_for_val(key)
		var group_key = "???"

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

		stats, ok := sl.Results[group_key]
		if !ok {
			stats = &SessionStats{}
			sl.Results[group_key] = stats
		}

		stats.CombineStats(&as.Stats)

	}

}

func (ss *SessionSpec) PrintResults() {
	log.Println("SESSION STATS")
	log.Println("UNIQUE SESSION IDS", len(ss.Sessions.List))

	log.Println("SESSIONS", ss.Count)
	log.Println("AVERAGE EVENTS PER SESSIONS", ss.Count/len(ss.Sessions.List))

	for key, s := range ss.Sessions.Results {
		s.PrintStats(key)
	}
}

func (ss *SessionSpec) CombineSessions(sessionspec *SessionSpec) {

	session_col := *FLAGS.SESSION_COL
	session_col_id := ss.block.get_key_id(session_col)
	col_info := sessionspec.block.GetColumnInfo(session_col_id)
	new_info := ss.block.GetColumnInfo(session_col_id)

	var sessionid string
	var newid int32
	for key, as := range sessionspec.Sessions.List {
		sessionid = col_info.get_string_for_val(key)
		newid = new_info.get_val_id(sessionid)

		prev_session, ok := ss.Sessions.List[newid]
		if !ok {
			ss.Sessions.List[newid] = as
		} else {
			prev_session.CombineSession(as)
		}
	}
}

func SessionizeRecords(querySpec *QuerySpec, sessionSpec *SessionSpec, recordsptr *RecordList) {
	records := *recordsptr
	for i := 0; i < len(records); i++ {
		r := records[i]

		session_col := *FLAGS.SESSION_COL
		field_id := r.block.get_key_id(session_col)
		var group_key int32
		switch r.Populated[field_id] {
		case INT_VAL:
			group_key = int32(r.Ints[field_id])

		case STR_VAL:
			group_key = int32(r.Strs[field_id])

		case _NO_VAL:
			log.Println("MISSING EVENT KEY!")

		}

		sessionSpec.Sessions.AddRecord(group_key, r)
	}

}

type SortBlocksByTime []*TableBlock

func (a SortBlocksByTime) Len() int      { return len(a) }
func (a SortBlocksByTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SortBlocksByTime) Less(i, j int) bool {
	time_col := *FLAGS.TIME_COL
	return a[i].Info.IntInfoMap[time_col].Min < a[j].Info.IntInfoMap[time_col].Min

}

var BLOCKS_BEFORE_GC = 8

func (t *Table) LoadAndSessionize(loadSpec *LoadSpec, querySpec *QuerySpec, sessionSpec *SessionSpec) int {

	loadSpec.Int(*FLAGS.TIME_COL)

	blocks := make(SortBlocksByTime, 0)
	for _, b := range t.BlockList {
		block := t.LoadBlockFromDir(b.Name, nil, false)
		if block != nil {
			if block.Info.IntInfoMap[*FLAGS.TIME_COL] != nil {
				blocks = append(blocks, block)

			}
		}

	}
	sort.Sort(SortBlocksByTime(blocks))
	log.Println("SORTED BLOCKS")

	masterSession := NewSessionSpec()
	// Setup the join table for the session spec
	if *FLAGS.JOIN_TABLE != "" {
		start := time.Now()
		log.Println("LOADING JOIN TABLE", *FLAGS.JOIN_TABLE)
		jt := GetTable(*FLAGS.JOIN_TABLE)
		masterSession.Sessions.JoinTable = jt

		loadSpec := jt.NewLoadSpec()
		loadSpec.LoadAllColumns = true

		DELETE_BLOCKS_AFTER_QUERY = false
		FLAGS.READ_INGESTION_LOG = &TRUE
		jt.LoadRecords(&loadSpec)
		end := time.Now()

		log.Println("LOADING JOIN TABLE TOOK", end.Sub(start))

		jt.BuildJoinMap()

	}

	masterSession.block = blocks[0]
	masterSession.Sessions.block = blocks[0]

	max_time := int64(0)
	count := 0

	var wg sync.WaitGroup

	result_lock := sync.Mutex{}
	count_lock := sync.Mutex{}

	for i, b := range blocks {

		min_time := b.Info.IntInfoMap[*FLAGS.TIME_COL].Min
		max_time = b.Info.IntInfoMap[*FLAGS.TIME_COL].Max
		this_block := b
		block_index := i
		wg.Add(1)
		go func() {

			//			log.Println("LOADING BLOCK", this_block.Name, min_time)
			fmt.Fprintf(os.Stderr, ".")
			blockQuery := CopyQuerySpec(querySpec)
			blockSession := NewSessionSpec()
			block := t.LoadBlockFromDir(this_block.Name, loadSpec, false)
			if block != nil {

				blockSession.block = block
				SessionizeRecords(blockQuery, &blockSession, &block.RecordList)
				count_lock.Lock()
				count += len(block.RecordList)
				count_lock.Unlock()
			}

			result_lock.Lock()
			masterSession.CombineSessions(&blockSession)
			delete(t.BlockList, block.Name)
			result_lock.Unlock()

			wg.Done()
		}()

		if block_index%BLOCKS_BEFORE_GC == 0 && block_index > 0 {
			wg.Wait()

			fmt.Fprintf(os.Stderr, "+")
			result_lock.Lock()
			masterSession.Sessions.NoMoreRecordsBefore(int(min_time))
			masterSession.ExpireRecords()
			result_lock.Unlock()
		}

	}

	wg.Wait()

	fmt.Fprintf(os.Stderr, "+")
	masterSession.Sessions.NoMoreRecordsBefore(int(max_time) + 2*SESSION_CUTOFF)
	masterSession.ExpireRecords()
	fmt.Fprintf(os.Stderr, "\n")
	log.Println("INSPECTED", count, "RECORDS")

	// Kick off the final grouping, aggregations and joining of sessions
	masterSession.Finalize()
	masterSession.PrintResults()

	return count

}
