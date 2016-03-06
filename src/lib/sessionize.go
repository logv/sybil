package sybil

import "log"
import "sort"
import "sync"

// NEXT:
// * pull group by key from an active session
// * make a join key for active sessions for adding new group by info
// * add session level aggregations: length, # events, etc into a Results Object according to the group key
//
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

type SessionSpec struct {
	ExpireAfter int // Seconds to expire a session after not seeing any new events

	block *TableBlock

	Sessions SessionList
	Count    int
}

func NewSessionSpec() SessionSpec {
	ss := SessionSpec{}

	ss.Sessions.List = make(Sessions)

	return ss
}

func (ss *SessionSpec) ExpireRecords() {
	ss.Count += ss.Sessions.ExpireRecords()
}

type Sessions map[int32]*ActiveSession
type SessionList struct {
	List Sessions

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
	NumEvents       int
	NumSessions     int
	SessionDuration int64

	SessionDelta   int64
	LastSessionEnd int64

	Calendar map[int]bool
}

var SINGLE_EVENT_DURATION = int64(30)

func (ss *SessionStats) SummarizeSession(records RecordList) {
	if len(records) == 0 {
		return
	}

	ss.NumEvents += len(records)
	ss.NumSessions++

	if ss.LastSessionEnd > 0 {
		ss.SessionDelta += records[0].Timestamp - ss.LastSessionEnd
	}

	if len(records) == 1 {
		ss.SessionDuration += SINGLE_EVENT_DURATION
		return
	}

	last_index := len(records) - 1
	delta := records[last_index].Timestamp - records[0].Timestamp
	ss.SessionDuration += delta

	ss.LastSessionEnd = records[last_index].Timestamp
}

func (as *ActiveSession) AddRecord(r *Record) {
	// TODO: Figure out where to put the record using sort indeces and slice insertion
	as.Records = append(as.Records, r)
}

func (as *ActiveSession) IsExpired() bool {

	return false
}

func (as *ActiveSession) Summarize() {
	log.Println("SUMMARIZING SESSION", len(as.Records))

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
		last_slice = as.Records[0:0]
		sessions = append(sessions, last_slice)
	}

	as.Records = last_slice

	for _, s := range sessions {
		as.Stats.SummarizeSession(s)
	}

	return sessions
}

func (sl *SessionList) AddRecord(group_key int32, r *Record) {
	session, ok := sl.List[group_key]
	if !ok {
		session = &ActiveSession{}
		session.Records = make(RecordList, 0)
		session.Stats = SessionStats{}
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

}

func (ss *SessionSpec) PrintResults() {

	log.Println("SESSION STATS")
	log.Println("UNIQUE IDS", len(ss.Sessions.List))
	log.Println("SESSIONS", ss.Count)
	log.Println("AVERAGE EVENTS PER SESSIONS", ss.Count/len(ss.Sessions.List))

	duration := float64(0)
	sessions := float64(0)
	for _, s := range ss.Sessions.List {
		if s.Stats.SessionDuration > 0 {
			duration += float64(s.Stats.SessionDuration) / float64(s.Stats.NumSessions) / float64(len(ss.Sessions.List))
			sessions += float64(s.Stats.NumSessions) / float64(len(ss.Sessions.List))
		}
	}

	log.Println("AVERAGE SESSION DURATION", duration)
	log.Println("AVERAGE SESSIONS PER PERSON", sessions)
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
	masterSession.block = blocks[0]

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

			log.Println("LOADING BLOCK", this_block.Name, min_time)
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

			log.Println("EXPIRING RECORDS BEFORE", min_time)
			result_lock.Lock()
			masterSession.Sessions.NoMoreRecordsBefore(int(min_time))
			masterSession.ExpireRecords()
			result_lock.Unlock()
		}

	}

	wg.Wait()

	log.Println("EXPIRING RECORDS BEFORE", max_time)
	masterSession.Sessions.NoMoreRecordsBefore(int(max_time))
	masterSession.ExpireRecords()
	log.Println("INSPECTED", count, "RECORDS")

	masterSession.Finalize() // Kick off the final aggregations and joining
	masterSession.PrintResults()

	return count

}
