#!/bin/sh
set -eu
python3 -m compileall -q registry_bridge
python3 -m unittest discover -s tests -v
python3 -m registry_bridge --db :memory: demo --fixture fixtures/demo.json
python3 -m synthetic_exchange.smoke
python3 -m synthetic_exchange.server --port 8765 &
server_pid=$!
trap 'kill "$server_pid" 2>/dev/null || true' EXIT
sleep 1
work=$(mktemp -d)
python3 -m registry_bridge --db "$work/feed.db" init >/dev/null
python3 -m registry_bridge --db "$work/feed.db" call issuer.add --json '{"issuer":"north","display_name":"Synthetic North"}' >/dev/null
python3 -m registry_bridge --db "$work/feed.db" call issuer.add --json '{"issuer":"south","display_name":"Synthetic South"}' >/dev/null
python3 -m registry_bridge --db "$work/feed.db" poll --source registry --url http://127.0.0.1:8765/changes --pages 4
python3 -m registry_bridge --db "$work/feed.db" poll --source registry --pages 2
python3 -m registry_bridge --db "$work/feed.db" checkpoints
python3 -m registry_bridge --db "$work/feed.db" report >/dev/null
