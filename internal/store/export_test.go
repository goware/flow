package store

// EventWaitUpdateQueryForTest exposes the exact production wait-update shape
// to external-package PostgreSQL plan tests without adding a production API.
func EventWaitUpdateQueryForTest(s *Store) string {
	return s.satisfyMatchingEventWaitsSQL()
}

func ActiveCommandListQueryForTest(s *Store, filter ActiveCommandListFilter) (string, []any) {
	return s.listActiveCommandsQuery(filter)
}

func KeyedHistoryListQueryForTest(s *Store, filter KeyedHistoryListFilter) (string, []any) {
	return s.listJournalByKeysQuery(filter)
}

func RunListQueryForTest(s *Store, filter RunListFilter) (string, []any) {
	return s.listRunsQuery(filter)
}

func QueueStatsQueryForTest(s *Store) string { return s.queueStatsQuery() }

func PruneCandidatesQueryForTest(s *Store) string { return s.pruneCandidatesQuery() }

func TraceWaitsQueryForTest(s *Store) string { return s.traceWaitsQuery() }
