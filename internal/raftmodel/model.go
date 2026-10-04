package raftmodel

type LogEntry struct {
	Term    uint64 `json:"term"`
	Index   uint64 `json:"index"`
	Command []byte `json:"command"`
}
