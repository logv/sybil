package edb

type SessionMap map[int][]Record;

func SessionizeRecords(records []*Record, session_field string) SessionMap {
  ret := make(SessionMap, 0);

  for i := 0; i < len(records); i++ {
    r := records[i];
  
    session_id, ok := r.getVal(session_field)

    if ok {
      ret[session_id] = append(ret[session_id], *r);
    }

  }

  return ret;
}

type SummaryFunc interface {
  summarize([]Record);
}

func SummarizeSessions(sessions SessionMap, summary_func SummaryFunc) {

}
