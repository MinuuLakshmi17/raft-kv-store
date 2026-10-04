package tests

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/MinuuLakshmi17/raft-kv-store/internal/kv"
	"github.com/MinuuLakshmi17/raft-kv-store/internal/raft"
	"github.com/MinuuLakshmi17/raft-kv-store/internal/rpc"
	"github.com/MinuuLakshmi17/raft-kv-store/internal/storage"
)

// These tests boot real 3-node clusters over gRPC on localhost. They cover
// the paths unit tests cannot: leader election, replication, failover, and
// snapshot catch-up.

const (
	testElectionMin = 150 * time.Millisecond
	testElectionMax = 300 * time.Millisecond
	testHeartbeat   = 50 * time.Millisecond
)

func testPeers() map[uint64]string {
	return map[uint64]string{
		1: "127.0.0.1:29101",
		2: "127.0.0.1:29102",
		3: "127.0.0.1:29103",
	}
}

// startTestNode boots one node; the returned func stops it.
func startTestNode(t *testing.T, id uint64, dir string, snapshotThreshold uint64) (*raft.Node, func()) {
	t.Helper()
	peers := testPeers()
	n, err := raft.NewNode(raft.Config{
		ID:                id,
		Peers:             peers,
		Storage:           storage.New(filepath.Join(dir, "raft.gob")),
		KV:                kv.NewStore(),
		ElectionMin:       testElectionMin,
		ElectionMax:       testElectionMax,
		Heartbeat:         testHeartbeat,
		Seed:              int64(id) * 7919,
		Dial:              rpc.Dial,
		SnapshotThreshold: snapshotThreshold,
	})
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", peers[id])
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = rpc.Serve(l, n) }()
	n.Start()
	return n, func() {
		n.Stop()
		l.Close()
	}
}

func waitForLeader(t *testing.T, nodes map[uint64]*raft.Node, timeout time.Duration) uint64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, n := range nodes {
			if n.IsLeader() {
				return id
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected in time")
	return 0
}

func writeKey(t *testing.T, n *raft.Node, key, value string) {
	t.Helper()
	cmd, _ := json.Marshal(kv.Command{Op: "set", Key: key, Value: value})
	idx, err := n.Submit(cmd)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if !n.WaitCommit(idx, 5*time.Second) {
		t.Fatalf("write %q was not committed", key)
	}
}

func waitForValue(t *testing.T, n *raft.Node, key, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if v, ok := n.KV().Get(key); ok && v == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("key %q=%q not visible in time", key, want)
}

// A healthy cluster elects exactly one leader and keeps it: no term churn.
func TestClusterElectsStableLeader(t *testing.T) {
	nodes := map[uint64]*raft.Node{}
	var stops []func()
	for id := uint64(1); id <= 3; id++ {
		n, stop := startTestNode(t, id, t.TempDir(), 0)
		nodes[id] = n
		stops = append(stops, stop)
	}
	defer func() {
		for _, s := range stops {
			s()
		}
	}()

	leader := waitForLeader(t, nodes, 10*time.Second)
	term := nodes[leader].Term()

	time.Sleep(1500 * time.Millisecond)

	if !nodes[leader].IsLeader() {
		t.Fatal("leader lost leadership without any failure")
	}
	if nodes[leader].Term() != term {
		t.Fatalf("term churned with no failures: %d -> %d", term, nodes[leader].Term())
	}
	leaders := 0
	for _, n := range nodes {
		if n.IsLeader() {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("want exactly 1 leader, have %d", leaders)
	}
}

// Writes replicate to every node; killing the leader fails over to a new
// one without losing committed writes; the old leader rejoins and catches up.
func TestClusterReplicatesAndSurvivesLeaderFailure(t *testing.T) {
	nodes := map[uint64]*raft.Node{}
	stops := map[uint64]func(){}
	dirs := map[uint64]string{}
	for id := uint64(1); id <= 3; id++ {
		dir := t.TempDir()
		dirs[id] = dir
		n, stop := startTestNode(t, id, dir, 0)
		nodes[id] = n
		stops[id] = stop
	}
	defer func() {
		for _, s := range stops {
			s()
		}
	}()

	leader := waitForLeader(t, nodes, 10*time.Second)
	writeKey(t, nodes[leader], "a", "1")
	for _, n := range nodes {
		waitForValue(t, n, "a", "1", 5*time.Second)
	}

	// Kill the leader. A new one must appear among the survivors.
	stops[leader]()
	delete(stops, leader)
	delete(nodes, leader)

	newLeader := waitForLeader(t, nodes, 10*time.Second)
	if newLeader == leader {
		t.Fatal("dead leader reported as leader")
	}
	writeKey(t, nodes[newLeader], "b", "2")
	for _, n := range nodes {
		waitForValue(t, n, "b", "2", 5*time.Second)
	}

	// Restart the old leader with its old data dir; it must catch up.
	n, stop := startTestNode(t, leader, dirs[leader], 0)
	nodes[leader] = n
	stops[leader] = stop
	waitForValue(t, n, "a", "1", 5*time.Second)
	waitForValue(t, n, "b", "2", 5*time.Second)
}

// With snapshots compacting the log, a node that lost everything must be
// brought current via InstallSnapshot, not log replay.
func TestSnapshotCatchUp(t *testing.T) {
	nodes := map[uint64]*raft.Node{}
	stops := map[uint64]func(){}
	dirs := map[uint64]string{}
	for id := uint64(1); id <= 3; id++ {
		dir := t.TempDir()
		dirs[id] = dir
		n, stop := startTestNode(t, id, dir, 4) // snapshot every 4 commits
		nodes[id] = n
		stops[id] = stop
	}
	defer func() {
		for _, s := range stops {
			s()
		}
	}()

	leader := waitForLeader(t, nodes, 10*time.Second)
	for i, k := range []string{"k1", "k2", "k3", "k4", "k5", "k6"} {
		writeKey(t, nodes[leader], k, string(rune('a'+i)))
	}
	for _, n := range nodes {
		waitForValue(t, n, "k6", "f", 5*time.Second)
	}

	// Wipe node 3 completely and restart it: its log is empty, the leader's
	// log is compacted, so catch-up must go through a snapshot.
	stops[3]()
	delete(stops, 3)
	delete(nodes, 3)
	if err := os.RemoveAll(dirs[3]); err != nil {
		t.Fatal(err)
	}
	n3, stop3 := startTestNode(t, 3, dirs[3], 4)
	nodes[3] = n3
	stops[3] = stop3

	waitForValue(t, n3, "k6", "f", 10*time.Second)
	// Spot-check a key that only exists inside the snapshot compacted range.
	waitForValue(t, n3, "k1", "a", 5*time.Second)
}
