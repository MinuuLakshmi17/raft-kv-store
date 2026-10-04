<div align="center">

# Raft KV Store

**A fault-tolerant distributed key-value store in Go, built on a from-scratch implementation of the Raft consensus protocol.**

[![Go 1.23](https://img.shields.io/badge/go-1.23-00ADD8.svg)](https://go.dev/)
[![gRPC](https://img.shields.io/badge/gRPC-1.68-244c5a.svg)](https://grpc.io/)
[![CI](https://github.com/MinuuLakshmi17/raft-kv-store/actions/workflows/ci.yml/badge.svg)](https://github.com/MinuuLakshmi17/raft-kv-store/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

</div>

---

## Overview

Three nodes, one shared truth. This is a distributed key-value store where every write is replicated through **Raft consensus** — the same algorithm behind etcd and Consul. Kill the leader mid-write and the cluster elects a new one without losing committed data. The consensus core is implemented from scratch (no Raft library); gRPC carries peer traffic with a hand-written JSON codec so the repo builds without generated protobuf code.

## Features

- **Raft consensus, implemented by hand** — leader election with randomized timeouts, log replication with conflict resolution, majority-quorum commit (current-term entries only, per Raft §5.4)
- **Durable writes** — the HTTP layer acknowledges a write only after it is committed on a majority; an acknowledged write survives `kill -9` on the leader
- **Crash recovery** — persistent term/vote/log via atomic file writes; on restart the log replays to rebuild state
- **Log compaction** — automatic snapshots every N committed entries, shipped to lagging followers via `InstallSnapshot`
- **Failure discovery** — followers answer `503` with the best-known leader ID so clients can retry against the new leader
- **Chaos-tested** — integration tests boot real 3-node clusters, kill the leader, and prove no committed write is lost

## Architecture

```
                    ┌──────────────┐
 Clients ──HTTP──▶  │    Leader    │──gRPC AppendEntries──▶ Follower
 (GET/PUT/DELETE)   │  Raft log    │──gRPC AppendEntries──▶ Follower
                    │  majority    │
                    │  commit ──▶ apply ──▶ KV state machine
                    └──────────────┘
```

Each node runs two servers: an HTTP server for client traffic and a gRPC server for peer consensus traffic. Committed log entries are applied in order to a deterministic in-memory KV state machine.

## Quick Start

Three terminals, one command each:

```bash
# Terminal 1
go run ./cmd/node -id 1 -raft :9001 -http :8001 -data ./data/node1 \
  -peers "1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003"

# Terminal 2
go run ./cmd/node -id 2 -raft :9002 -http :8002 -data ./data/node2 \
  -peers "1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003"

# Terminal 3
go run ./cmd/node -id 3 -raft :9003 -http :8003 -data ./data/node3 \
  -peers "1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003"
```

Find the leader and write a key:

```bash
curl http://localhost:8001/leader   # check each node; one reports "is_leader":true
curl -X PUT http://localhost:8002/kv/greeting \
  -H 'content-type: application/json' -d '{"value":"hello"}'
# {"index":1,"status":"committed"}

curl http://localhost:8001/kv/greeting   # readable from any node
# {"key":"greeting","value":"hello"}
```

## Chaos Demo

The moment this project is built for — kill the leader and watch the cluster carry on:

1. Write a key through the leader: `PUT /kv/before {"value":"survives"}`
2. `kill -9` the leader's PID
3. Poll `/leader` on the survivors until one reports `"is_leader":true`
4. Write another key through the new leader
5. Restart the old node with its same `-data` directory
6. `GET` both keys on all three nodes — both present, nothing lost

`scripts/chaos.sh` documents the exact steps.

## HTTP API

| Method | Path | Description |
|---|---|---|
| `PUT` | `/kv/{key}` | Set a key (body: `{"value":"..."}`). Leader only; waits for majority commit. |
| `GET` | `/kv/{key}` | Get a key from any node. |
| `DELETE` | `/kv/{key}` | Delete a key. Leader only. |
| `GET` | `/leader` | This node's view: `is_leader`, `leader_id`, `term`. |
| `GET` | `/health` | Liveness probe with node role and term. |
| `POST` | `/snapshot` | Trigger a manual snapshot. |

Writes to a follower return `503` with the known `leader_id` so clients can retry against the leader.

## Testing

```bash
go test ./...        # unit + 3-node integration tests
go test -race ./...  # race detector
```

The integration suite (`tests/cluster_test.go`) boots real clusters over gRPC and verifies:

- **Stable election** — exactly one leader, no term churn without failures
- **Failover** — killing the leader elects a new one; committed writes survive; the old node rejoins and catches up
- **Snapshot catch-up** — a node wiped clean is brought current via `InstallSnapshot`

## Configuration

| Flag | Default | Description |
|---|---|---|
| `-id` | `1` | This node's ID |
| `-raft` | `:9001` | gRPC peer address |
| `-http` | `:8001` | HTTP client address |
| `-peers` | `1=127.0.0.1:9001,...` | `id=addr` peer list |
| `-data` | `./data/node1` | Data directory (Raft state) |
| `-snapshot-threshold` | `1000` | Auto-snapshot every N committed entries (`0` disables) |

## Project Structure

```
cmd/node/            # node binary: flags, HTTP API, wiring
internal/raft/       # Raft core: elections, replication, commit, snapshots
internal/rpc/        # gRPC transport (hand-rolled service def, JSON codec)
internal/kv/         # deterministic KV state machine
internal/storage/    # atomic persistent state (term, vote, log)
internal/wire/       # RPC message types
internal/raftmodel/  # log entry model
proto/raft.proto     # documented wire contract
tests/               # unit + 3-node integration tests
scripts/chaos.sh     # chaos drill walkthrough
```

Design notes: [docs/DESIGN.md](docs/DESIGN.md)

## Scope

This is an educational systems project, not a production database — it implements consensus itself rather than importing a Raft library. Production hardening would additionally require membership changes, streaming snapshots, linearizable read barriers/leases, backpressure, TLS/authenticated peer transport, fsync'd crash consistency, and exhaustive model checking.

## License

MIT — see [LICENSE](LICENSE).
