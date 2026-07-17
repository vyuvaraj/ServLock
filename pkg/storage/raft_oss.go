//go:build !enterprise

package storage

func (s *InMemoryStore) replicateState() {
	// Open-source version does not replicate state to peers
}
