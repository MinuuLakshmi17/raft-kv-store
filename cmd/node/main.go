package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/MinuuLakshmi17/raft-kv-store/internal/kv"
	"github.com/MinuuLakshmi17/raft-kv-store/internal/raft"
	"github.com/MinuuLakshmi17/raft-kv-store/internal/rpc"
	"github.com/MinuuLakshmi17/raft-kv-store/internal/storage"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// writeNotLeader answers 503 with the best-known leader so clients can retry
// against it. leader_id is 0 when this node doesn't know of one yet.
func writeNotLeader(w http.ResponseWriter, n *raft.Node) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	json.NewEncoder(w).Encode(map[string]interface{}{"error": "not leader", "leader_id": n.LeaderID()})
}

func peers(s string) map[uint64]string {
	m := map[uint64]string{}
	for _, p := range strings.Split(s, ",") {
		x := strings.SplitN(p, "=", 2)
		if len(x) == 2 {
			id, _ := strconv.ParseUint(x[0], 10, 64)
			m[id] = x[1]
		}
	}
	return m
}
func main() {
	id := flag.Uint64("id", 1, "id")
	ra := flag.String("raft", ":9001", "raft addr")
	ha := flag.String("http", ":8001", "http addr")
	pa := flag.String("peers", "1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003", "peers")
	da := flag.String("data", "./data/node1", "data")
	st := flag.Uint64("snapshot-threshold", 1000, "auto-snapshot every N committed entries (0 disables)")
	flag.Parse()
	n, e := raft.NewNode(raft.Config{ID: *id, Peers: peers(*pa), Storage: storage.New(*da + "/raft.gob"), KV: kv.NewStore(), ElectionMin: 500 * time.Millisecond, ElectionMax: 900 * time.Millisecond, Heartbeat: 120 * time.Millisecond, Seed: int64(*id), Dial: rpc.Dial, SnapshotThreshold: *st})
	if e != nil {
		panic(e)
	}
	n.Start()
	l, e := net.Listen("tcp", *ra)
	if e != nil {
		panic(e)
	}
	go func() {
		if e := rpc.Serve(l, n); e != nil {
			panic(e)
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{"status": "healthy", "node": *id, "term": n.Term(), "leader": n.IsLeader()})
	})
	mux.HandleFunc("/leader", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{"node": *id, "is_leader": n.IsLeader(), "leader_id": n.LeaderID(), "term": n.Term()})
	})
	// submitWrite appends a command and waits until it is committed on a
	// majority. A write is only acknowledged once it is durable: an entry
	// sitting solely on the leader can still be lost.
	submitWrite := func(w http.ResponseWriter, cmd []byte) {
		i, e := n.Submit(cmd)
		if e != nil {
			writeNotLeader(w, n)
			return
		}
		if !n.WaitCommit(i, 5*time.Second) {
			http.Error(w, "write not committed in time", 503)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"index": i, "status": "committed"})
	}
	mux.HandleFunc("/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "method not allowed", 405)
			return
		}
		if e := n.CreateSnapshot(); e != nil {
			http.Error(w, e.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "snapshot created"})
	})
	mux.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		k := strings.TrimPrefix(r.URL.Path, "/kv/")
		if r.Method == "GET" {
			v, ok := n.KV().Get(k)
			if !ok {
				http.Error(w, "not found", 404)
				return
			}
			json.NewEncoder(w).Encode(map[string]string{"key": k, "value": v})
			return
		}
		if !n.IsLeader() {
			writeNotLeader(w, n)
			return
		}
		var b struct {
			Value string `json:"value"`
		}
		if r.Method == "PUT" {
			if json.NewDecoder(r.Body).Decode(&b) != nil {
				http.Error(w, "bad json", 400)
				return
			}
			c, _ := json.Marshal(kv.Command{Op: "set", Key: k, Value: b.Value})
			submitWrite(w, c)
			return
		}
		if r.Method == "DELETE" {
			c, _ := json.Marshal(kv.Command{Op: "delete", Key: k})
			submitWrite(w, c)
			return
		}
	})
	fmt.Printf("node %d raft=%s http=%s\n", *id, *ra, *ha)
	if e := http.ListenAndServe(*ha, mux); e != nil {
		panic(e)
	}
}
