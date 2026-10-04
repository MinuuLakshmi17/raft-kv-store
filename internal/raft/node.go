package raft

import (
	"context"
	"errors"
	"github.com/MinuuLakshmi17/raft-kv-store/internal/kv"
	"github.com/MinuuLakshmi17/raft-kv-store/internal/raftmodel"
	"github.com/MinuuLakshmi17/raft-kv-store/internal/storage"
	"github.com/MinuuLakshmi17/raft-kv-store/internal/wire"
	"math/rand"
	"sync"
	"time"
)

type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

type LogEntry = raftmodel.LogEntry
type VoteResult struct {
	Term        uint64
	VoteGranted bool
}
type AppendResult struct {
	Term                        uint64
	Success                     bool
	ConflictIndex, ConflictTerm uint64
}
type SnapshotResult struct{ Term uint64 }
type Peer interface {
	RequestVote(context.Context, *wire.RequestVoteRequest) (*wire.RequestVoteResponse, error)
	AppendEntries(context.Context, *wire.AppendEntriesRequest) (*wire.AppendEntriesResponse, error)
	InstallSnapshot(context.Context, *wire.InstallSnapshotRequest) (*wire.InstallSnapshotResponse, error)
	Close() error
}
type Dialer func(context.Context, string) (Peer, error)
type Config struct {
	ID                                  uint64
	Peers                               map[uint64]string
	Storage                             *storage.Store
	KV                                  *kv.Store
	ElectionMin, ElectionMax, Heartbeat time.Duration
	Seed                                int64
	Dial                                Dialer
	// SnapshotThreshold triggers an automatic snapshot once this many
	// committed entries sit after the last snapshot. 0 disables it.
	SnapshotThreshold uint64
}
type Node struct {
	mu                                  sync.Mutex
	id                                  uint64
	peers                               map[uint64]string
	store                               *storage.Store
	kv                                  *kv.Store
	role                                Role
	leaderID                            uint64 // best-known leader, 0 = unknown
	currentTerm, votedFor               uint64
	log                                 []LogEntry
	commitIndex, lastApplied            uint64
	snapshotIndex, snapshotTerm         uint64
	snapshotData                        []byte
	snapshotThreshold                   uint64
	nextIndex, matchIndex               map[uint64]uint64
	electionMin, electionMax, heartbeat time.Duration
	rng                                 *rand.Rand
	dial                                Dialer
	reset                               chan struct{}
	stop                                chan struct{}
	stopped                             chan struct{}
}

func NewNode(c Config) (*Node, error) {
	st, e := c.Storage.Load()
	if e != nil {
		return nil, e
	}
	if c.ElectionMin == 0 {
		c.ElectionMin = 400 * time.Millisecond
	}
	if c.ElectionMax == 0 {
		c.ElectionMax = 800 * time.Millisecond
	}
	if c.Heartbeat == 0 {
		c.Heartbeat = 100 * time.Millisecond
	}
	n := &Node{id: c.ID, peers: c.Peers, store: c.Storage, kv: c.KV, currentTerm: st.CurrentTerm, votedFor: st.VotedFor, log: st.Log, commitIndex: st.CommitIndex, lastApplied: st.LastApplied, snapshotIndex: st.SnapshotIndex, snapshotTerm: st.SnapshotTerm, snapshotData: st.SnapshotData, snapshotThreshold: c.SnapshotThreshold, electionMin: c.ElectionMin, electionMax: c.ElectionMax, heartbeat: c.Heartbeat, rng: rand.New(rand.NewSource(c.Seed)), dial: c.Dial, reset: make(chan struct{}, 1), stop: make(chan struct{}), stopped: make(chan struct{})}
	if len(n.log) == 0 {
		n.log = []LogEntry{{Index: 0, Term: 0}}
	}
	if len(n.snapshotData) > 0 {
		_ = n.kv.Restore(n.snapshotData)
	} else {
		// No snapshot: the in-memory state machine starts empty while the
		// persisted log still holds every applied entry. Replay the log
		// from the beginning to rebuild it; without this, a restart would
		// silently drop all applied state (lastApplied is persisted, so
		// applyLocked would otherwise skip everything).
		n.lastApplied = 0
	}
	n.applyLocked()
	return n, nil
}
func (n *Node) ID() uint64     { return n.id }
func (n *Node) Role() Role     { n.mu.Lock(); defer n.mu.Unlock(); return n.role }
func (n *Node) Term() uint64   { n.mu.Lock(); defer n.mu.Unlock(); return n.currentTerm }
func (n *Node) IsLeader() bool { return n.Role() == Leader }
func (n *Node) LeaderID() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role == Leader {
		return n.id
	}
	return n.leaderID
}
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// WaitCommit blocks until the given log index is committed or the timeout
// elapses. Writers must wait for commit before acknowledging a write:
// an entry sitting only on the leader is not yet durable.
func (n *Node) WaitCommit(index uint64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if n.CommitIndex() >= index {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func (n *Node) KV() *kv.Store { return n.kv }
func (n *Node) Start() {
	go n.electionLoop()
	go n.heartbeatLoop()
}
func (n *Node) Stop() {
	select {
	case <-n.stop:
		return
	default:
		close(n.stop)
	}
	select {
	case <-n.stopped:
	case <-time.After(time.Second):
	}
}
func (n *Node) persistLocked() error {
	return n.store.Save(storage.PersistentState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor, Log: n.log, CommitIndex: n.commitIndex, LastApplied: n.lastApplied, SnapshotIndex: n.snapshotIndex, SnapshotTerm: n.snapshotTerm, SnapshotData: n.snapshotData})
}
func (n *Node) timeout() time.Duration {
	d := n.electionMax - n.electionMin
	return n.electionMin + time.Duration(n.rng.Int63n(int64(d)))
}
func (n *Node) resetTimer() {
	select {
	case n.reset <- struct{}{}:
	default:
	}
}
func (n *Node) electionLoop() {
	defer close(n.stopped)
	t := time.NewTimer(n.timeout())
	defer t.Stop()
	for {
		select {
		case <-n.stop:
			return
		case <-n.reset:
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			t.Reset(n.timeout())
		case <-t.C:
			// Only followers and candidates time out into elections.
			// A leader that ran its election timer would step itself down
			// and churn terms forever.
			if n.Role() != Leader {
				n.startElection()
			}
			t.Reset(n.timeout())
		}
	}
}
func (n *Node) startElection() {
	n.mu.Lock()
	n.role = Candidate
	n.currentTerm++
	term := n.currentTerm
	n.votedFor = n.id
	li, lt := n.lastLocked()
	_ = n.persistLocked()
	n.mu.Unlock()
	votes := 1
	var wg sync.WaitGroup
	var vm sync.Mutex
	for id, addr := range n.peers {
		if id == n.id {
			continue
		}
		wg.Add(1)
		go func(id uint64, addr string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			if n.dial == nil {
				return
			}
			c, err := n.dial(ctx, addr)
			if err != nil {
				return
			}
			defer c.Close()
			resp, err := c.RequestVote(ctx, &wire.RequestVoteRequest{Term: term, CandidateID: n.id, LastLogIndex: li, LastLogTerm: lt})
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if resp.Term > n.currentTerm {
				n.followerLocked(resp.Term)
				return
			}
			if n.role == Candidate && n.currentTerm == term && resp.VoteGranted {
				vm.Lock()
				votes++
				won := votes > len(n.peers)/2
				vm.Unlock()
				if won {
					n.leaderLocked()
				}
			}
			_ = id
		}(id, addr)
	}
	wg.Wait()
	n.resetTimer()
}
func (n *Node) followerLocked(term uint64) {
	n.role = Follower
	n.currentTerm = term
	n.votedFor = 0
	n.leaderID = 0
	_ = n.persistLocked()
	n.resetTimer()
}
func (n *Node) leaderLocked() {
	if n.role == Leader {
		return
	}
	n.role = Leader
	n.leaderID = n.id
	n.nextIndex = map[uint64]uint64{}
	n.matchIndex = map[uint64]uint64{}
	next := n.lastIndexLocked() + 1
	for id := range n.peers {
		n.nextIndex[id] = next
	}
}
func (n *Node) heartbeatLoop() {
	t := time.NewTicker(n.heartbeat)
	defer t.Stop()
	for {
		select {
		case <-n.stop:
			return
		case <-t.C:
			n.broadcast()
		}
	}
}
func (n *Node) broadcast() {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	peers := map[uint64]string{}
	for id, a := range n.peers {
		peers[id] = a
	}
	n.mu.Unlock()
	for id, a := range peers {
		if id != n.id {
			go n.replicate(id, a, term)
		}
	}
}
func (n *Node) replicate(id uint64, a string, term uint64) {
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	// The follower needs entries we have already compacted: ship the
	// whole snapshot instead of log entries that no longer exist.
	if n.snapshotIndex > 0 && n.nextIndex[id] <= n.snapshotIndex {
		idx, tm, data := n.snapshotIndex, n.snapshotTerm, n.snapshotData
		n.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c, e := n.dial(ctx, a)
		if e != nil {
			return
		}
		defer c.Close()
		r, e := c.InstallSnapshot(ctx, &wire.InstallSnapshotRequest{Term: term, LeaderID: n.id, LastIncludedIndex: idx, LastIncludedTerm: tm, Data: data})
		if e != nil {
			return
		}
		n.mu.Lock()
		defer n.mu.Unlock()
		if r.Term > n.currentTerm {
			n.followerLocked(r.Term)
			return
		}
		if n.role != Leader || n.currentTerm != term {
			return
		}
		n.matchIndex[id] = idx
		n.nextIndex[id] = idx + 1
		return
	}
	next := n.nextIndex[id]
	if next == 0 {
		next = 1
	}
	prev := next - 1
	pt := n.termAtLocked(prev)
	es := n.entriesFromLocked(next)
	commit := n.commitIndex
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	c, e := n.dial(ctx, a)
	if e != nil {
		return
	}
	defer c.Close()
	r, e := c.AppendEntries(ctx, &wire.AppendEntriesRequest{Term: term, LeaderID: n.id, PrevLogIndex: prev, PrevLogTerm: pt, Entries: es, LeaderCommit: commit})
	if e != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if r.Term > n.currentTerm {
		n.followerLocked(r.Term)
		return
	}
	if n.role != Leader || n.currentTerm != term {
		return
	}
	if r.Success {
		n.matchIndex[id] = prev + uint64(len(es))
		n.nextIndex[id] = n.matchIndex[id] + 1
		n.advanceLocked()
	} else if n.nextIndex[id] > 1 {
		n.nextIndex[id]--
	}
}
func (n *Node) HandleRequestVote(term, candidate, li, lt uint64) VoteResult {
	n.mu.Lock()
	defer n.mu.Unlock()
	if term < n.currentTerm {
		return VoteResult{n.currentTerm, false}
	}
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = 0
		n.role = Follower
		// Persist the term move even when the vote is denied: a crash
		// before persisting could otherwise let this node vote twice
		// across the restart.
		_ = n.persistLocked()
	}
	up := lt > n.lastLogTermLocked() || (lt == n.lastLogTermLocked() && li >= n.lastIndexLocked())
	grant := (n.votedFor == 0 || n.votedFor == candidate) && up
	if grant {
		n.votedFor = candidate
		_ = n.persistLocked()
		n.resetTimer()
	}
	return VoteResult{n.currentTerm, grant}
}
func (n *Node) HandleAppendEntries(term, leader, pi, pt uint64, es []LogEntry, lc uint64) AppendResult {
	n.mu.Lock()
	defer n.mu.Unlock()
	if term < n.currentTerm {
		return AppendResult{n.currentTerm, false, 0, 0}
	}
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = 0
	}
	n.role = Follower
	n.leaderID = leader
	n.resetTimer()
	if pi > n.lastIndexLocked() {
		return AppendResult{n.currentTerm, false, n.lastIndexLocked() + 1, 0}
	}
	if n.termAtLocked(pi) != pt {
		return AppendResult{n.currentTerm, false, pi, n.termAtLocked(pi)}
	}
	for _, e := range es {
		if e.Index <= n.lastIndexLocked() && n.termAtLocked(e.Index) != e.Term {
			n.truncateLocked(e.Index)
		}
		if e.Index > n.lastIndexLocked() {
			n.log = append(n.log, e)
		}
	}
	if lc > n.commitIndex {
		n.commitIndex = min(lc, n.lastIndexLocked())
		n.applyLocked()
	}
	_ = n.persistLocked()
	return AppendResult{n.currentTerm, true, 0, 0}
}
func (n *Node) Submit(cmd []byte) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return 0, errors.New("not leader")
	}
	i := n.lastIndexLocked() + 1
	n.log = append(n.log, LogEntry{Term: n.currentTerm, Index: i, Command: append([]byte(nil), cmd...)})
	_ = n.persistLocked()
	return i, nil
}
func (n *Node) advanceLocked() {
	for i := n.lastIndexLocked(); i > n.commitIndex; i-- {
		if n.termAtLocked(i) != n.currentTerm {
			continue
		}
		c := 1
		for id := range n.peers {
			if id != n.id && n.matchIndex[id] >= i {
				c++
			}
		}
		if c > len(n.peers)/2 {
			n.commitIndex = i
			n.applyLocked()
			_ = n.persistLocked()
			return
		}
	}
}
func (n *Node) applyLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		if n.lastApplied <= n.snapshotIndex {
			continue
		}
		e := n.entryAtLocked(n.lastApplied)
		if e != nil {
			_ = n.kv.Apply(e.Command)
		}
	}
	// Compact the log once enough committed entries pile up past the
	// last snapshot. Followers that need compacted entries are shipped
	// a snapshot by the leader (see replicate).
	if n.snapshotThreshold > 0 && n.commitIndex-n.snapshotIndex >= n.snapshotThreshold {
		_ = n.createSnapshotLocked()
	}
}
func (n *Node) CreateSnapshot() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.createSnapshotLocked()
}

func (n *Node) createSnapshotLocked() error {
	if n.commitIndex <= n.snapshotIndex {
		return nil
	}
	d, e := n.kv.Snapshot()
	if e != nil {
		return e
	}
	idx := n.commitIndex
	term := n.termAtLocked(idx)
	suffix := []LogEntry{}
	for _, x := range n.log {
		if x.Index > idx {
			suffix = append(suffix, x)
		}
	}
	n.snapshotIndex = idx
	n.snapshotTerm = term
	n.snapshotData = d
	n.log = append([]LogEntry{{Index: idx, Term: term}}, suffix...)
	return n.persistLocked()
}
func (n *Node) HandleInstallSnapshot(term, leader, idx, it uint64, d []byte) SnapshotResult {
	n.mu.Lock()
	defer n.mu.Unlock()
	if term < n.currentTerm {
		return SnapshotResult{n.currentTerm}
	}
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = 0
	}
	n.role = Follower
	n.leaderID = leader
	n.resetTimer()
	if idx > n.snapshotIndex {
		n.snapshotIndex = idx
		n.snapshotTerm = it
		n.snapshotData = d
		_ = n.kv.Restore(d)
		n.log = []LogEntry{{Index: idx, Term: it}}
	}
	if n.commitIndex < idx {
		n.commitIndex = idx
	}
	if n.lastApplied < idx {
		n.lastApplied = idx
	}
	_ = n.persistLocked()
	return SnapshotResult{n.currentTerm}
}
func (n *Node) lastIndexLocked() uint64      { return n.log[len(n.log)-1].Index }
func (n *Node) lastLogTermLocked() uint64    { return n.termAtLocked(n.lastIndexLocked()) }
func (n *Node) lastLocked() (uint64, uint64) { i := n.lastIndexLocked(); return i, n.termAtLocked(i) }
func (n *Node) termAtLocked(i uint64) uint64 {
	if i == n.snapshotIndex {
		return n.snapshotTerm
	}
	for _, e := range n.log {
		if e.Index == i {
			return e.Term
		}
	}
	return 0
}
func (n *Node) entryAtLocked(i uint64) *LogEntry {
	for x := range n.log {
		if n.log[x].Index == i {
			return &n.log[x]
		}
	}
	return nil
}
func (n *Node) entriesFromLocked(i uint64) []LogEntry {
	out := []LogEntry{}
	for _, e := range n.log {
		if e.Index >= i {
			out = append(out, e)
		}
	}
	return out
}
func (n *Node) truncateLocked(i uint64) {
	out := []LogEntry{}
	for _, e := range n.log {
		if e.Index < i {
			out = append(out, e)
		}
	}
	n.log = out
}
func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
