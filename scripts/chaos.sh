#!/usr/bin/env bash
set -euo pipefail
cat <<'TXT'
CHAOS DEMO
1. Start three nodes using README commands.
2. Discover leader: curl http://localhost:8001/leader ; repeat for 8002/8003.
3. Write: curl -X PUT localhost:<leader>/kv/before -H 'content-type: application/json' -d '{"value":"survives"}'
4. Identify leader PID and run: kill -9 <PID>.
5. Poll /leader on the surviving nodes until one reports is_leader=true.
6. Write a second key through the new leader.
7. Restart the killed node with its same data directory.
8. Verify GETs for both keys after catch-up.
TXT
