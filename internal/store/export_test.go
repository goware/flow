package store

// EventWaitUpdateQueryForTest exposes the exact production wait-update shape
// to external-package PostgreSQL plan tests without adding a production API.
func EventWaitUpdateQueryForTest(s *Store) string {
	return s.satisfyMatchingEventWaitsSQL()
}
