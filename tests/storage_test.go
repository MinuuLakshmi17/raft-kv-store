package tests

import (
	"github.com/MinuuLakshmi17/raft-kv-store/internal/raftmodel"
	"github.com/MinuuLakshmi17/raft-kv-store/internal/storage"
	"path/filepath"
	"testing"
)

func TestStorageRoundTrip(t *testing.T) {
	s := storage.New(filepath.Join(t.TempDir(), "state.gob"))
	in := storage.PersistentState{CurrentTerm: 3, VotedFor: 2, Log: []raftmodel.LogEntry{{Index: 0, Term: 0}, {Index: 1, Term: 3}}, CommitIndex: 1}
	if e := s.Save(in); e != nil {
		t.Fatal(e)
	}
	out, e := s.Load()
	if e != nil {
		t.Fatal(e)
	}
	if out.CurrentTerm != 3 || out.VotedFor != 2 || out.CommitIndex != 1 {
		t.Fatalf("%+v", out)
	}
}
