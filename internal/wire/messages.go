package wire

import "github.com/MinuuLakshmi17/raft-kv-store/internal/raftmodel"

type RequestVoteRequest struct{ Term, CandidateID, LastLogIndex, LastLogTerm uint64 }
type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}
type AppendEntriesRequest struct {
	Term, LeaderID, PrevLogIndex, PrevLogTerm uint64
	Entries                                   []raftmodel.LogEntry
	LeaderCommit                              uint64
}
type AppendEntriesResponse struct {
	Term                        uint64
	Success                     bool
	ConflictIndex, ConflictTerm uint64
}
type InstallSnapshotRequest struct {
	Term, LeaderID, LastIncludedIndex, LastIncludedTerm uint64
	Data                                                []byte
}
type InstallSnapshotResponse struct{ Term uint64 }
