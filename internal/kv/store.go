package kv

import (
	"encoding/json"
	"sync"
)

type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStore() *Store { return &Store{data: map[string]string{}} }
func (s *Store) Apply(raw []byte) error {
	var c Command
	if err := json.Unmarshal(raw, &c); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch c.Op {
	case "set":
		s.data[c.Key] = c.Value
	case "delete":
		delete(s.data, c.Key)
	}
	return nil
}
func (s *Store) Get(k string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[k]
	return v, ok
}
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.data)
}
func (s *Store) Restore(b []byte) error {
	var m map[string]string
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = m
	return nil
}
