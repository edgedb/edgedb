#!/bin/bash -ex

DIR="$1"
shift
PORT=12346

edb inittestdb -D "$DIR" "$@"
edb server -D "$DIR" -P $PORT &
SPID=$!

EDGEDB_PORT=$PORT EDGEDB_CLIENT_TLS_SECURITY=insecure python3 tests/inplace-testing/prep-upgrades.py > "${DIR}/upgrade.json"

kill $SPID
wait $SPID
