package tests

import (
	"encoding/json"
	"github.com/MinuuLakshmi17/raft-kv-store/internal/kv"
	"github.com/MinuuLakshmi17/raft-kv-store/internal/raft"
	"github.com/MinuuLakshmi17/raft-kv-store/internal/storage"
	"path/filepath"
	"testing"
	"time"
)

func node(t *testing.T, id uint64) *raft.Node {
	t.Helper()
	n, e := raft.NewNode(raft.Config{ID: id, Peers: map[uint64]string{id: "unused"}, Storage: storage.New(filepath.Join(t.TempDir(), "state.gob")), KV: kv.NewStore(), ElectionMin: time.Second, ElectionMax: 2 * time.Second, Seed: int64(id)})
	if e != nil {
		t.Fatal(e)
	}
	return n
}
func TestVoteOnce(t *testing.T) {
	n := node(t, 1)
	if !n.HandleRequestVote(1, 2, 0, 0).VoteGranted {
		t.Fatal()
	}
	if n.HandleRequestVote(1, 3, 0, 0).VoteGranted {
		t.Fatal()
	}
}
func TestAppendAppliesCommitted(t *testing.T) {
	n := node(t, 1)
	c, _ := json.Marshal(kv.Command{Op: "set", Key: "a", Value: "b"})
	r := n.HandleAppendEntries(1, 2, 0, 0, []raft.LogEntry{{Index: 1, Term: 1, Command: c}}, 1)
	if !r.Success {
		t.Fatal()
	}
	v, ok := n.KV().Get("a")
	if !ok || v != "b" {
		t.Fatalf("%q %v", v, ok)
	}
}
func TestSnapshotRestore(t *testing.T) {
	n := node(t, 1)
	c, _ := json.Marshal(kv.Command{Op: "set", Key: "x", Value: "42"})
	n.HandleAppendEntries(1, 2, 0, 0, []raft.LogEntry{{Index: 1, Term: 1, Command: c}}, 1)
	if e := n.CreateSnapshot(); e != nil {
		t.Fatal(e)
	}
	v, ok := n.KV().Get("x")
	if !ok || v != "42" {
		t.Fatal()
	}
}
