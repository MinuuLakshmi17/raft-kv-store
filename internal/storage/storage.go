package storage

import (
	"encoding/gob"
	"github.com/MinuuLakshmi17/raft-kv-store/internal/raftmodel"
	"os"
	"path/filepath"
	"sync"
)

type PersistentState struct {
	CurrentTerm, VotedFor       uint64
	Log                         []raftmodel.LogEntry
	CommitIndex, LastApplied    uint64
	SnapshotIndex, SnapshotTerm uint64
	SnapshotData                []byte
}
type Store struct {
	mu   sync.Mutex
	path string
}

func New(p string) *Store { return &Store{path: p} }
func (s *Store) Save(st PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := os.MkdirAll(filepath.Dir(s.path), 0755); e != nil {
		return e
	}
	tmp := s.path + ".tmp"
	f, e := os.Create(tmp)
	if e != nil {
		return e
	}
	if e = gob.NewEncoder(f).Encode(st); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	return os.Rename(tmp, s.path)
}
func (s *Store) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, e := os.Open(s.path)
	if os.IsNotExist(e) {
		return PersistentState{}, nil
	}
	if e != nil {
		return PersistentState{}, e
	}
	defer f.Close()
	var st PersistentState
	e = gob.NewDecoder(f).Decode(&st)
	return st, e
}
