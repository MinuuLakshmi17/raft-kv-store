# Raft KV Store

A from-scratch 3-node distributed key-value store in Go implementing Raft-style leader election, log replication, quorum commit, durable state, snapshots, gRPC peer transport, and a chaos-test workflow.

## Run tests

```bash
go mod tidy
go test ./...
go test -race ./...
```

## Run a 3-node cluster

Terminal 1:
```bash
go run ./cmd/node -id 1 -raft :9001 -http :8001 -data ./data/node1 -peers "1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003"
```
Terminal 2:
```bash
go run ./cmd/node -id 2 -raft :9002 -http :8002 -data ./data/node2 -peers "1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003"
```
Terminal 3:
```bash
go run ./cmd/node -id 3 -raft :9003 -http :8003 -data ./data/node3 -peers "1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003"
```

Use `/leader` to discover the current leader and `/kv/{key}` for GET/PUT/DELETE. A write sent to a follower returns 503 with its known leader when available. Writes are acknowledged only after the entry is committed on a majority (`{"status":"committed"}`), so an acknowledged write survives the leader being killed. `POST /snapshot` triggers a manual snapshot; snapshots are also taken automatically every `--snapshot-threshold` committed entries (default 1000) and shipped to lagging followers.

## Chaos demo

Run the three nodes, discover the leader, write a key, `kill -9` the leader, wait for a new leader, write another key, restart the old node, and verify both committed values. `scripts/chaos.sh` documents the exact curl workflow.

## Scope

The repository deliberately implements consensus logic itself rather than importing a Raft library. It is an educational systems project, not a production database. Production hardening would additionally require membership changes, streaming snapshots, linearizable read barriers/leases, backpressure, TLS/authenticated peer transport, stronger crash-consistency guarantees, and exhaustive model checking.
