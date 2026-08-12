package store

// EventWaitUpdateQueryForTest exposes the exact production wait-update shape
// to external-package PostgreSQL plan tests without adding a production API.
func EventWaitUpdateQueryForTest(s *Store) string {
	return s.satisfyMatchingEventWaitsSQL()
}

func LiveWorkListQueryForTest(s *Store, filter LiveWorkListFilter) (string, []any) {
	return s.listLiveWorkQuery(filter)
}

func KeyedHistoryListQueryForTest(s *Store, filter KeyedHistoryListFilter) (string, []any) {
	return s.listJournalByKeysQuery(filter)
}

func RunListQueryForTest(s *Store, filter RunListFilter) (string, []any) {
	return s.listRunsQuery(filter)
}

func QueueDepthQueryForTest(s *Store) string { return s.queueDepthQuery() }

func TraceWaitsQueryForTest(s *Store) string { return s.traceWaitsQuery() }
