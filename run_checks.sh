#!/bin/sh
set -eu
python3 -m compileall -q registry_bridge
python3 -m unittest discover -s tests -v
python3 -m registry_bridge --db :memory: demo --fixture fixtures/demo.json
python3 -m synthetic_exchange.smoke
