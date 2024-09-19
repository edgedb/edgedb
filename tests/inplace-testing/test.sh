#!/bin/bash -ex

while [[ $# -gt 0 ]]; do
  case $1 in
    --rollback-and-test)
        ROLLBACK=1
        shift
        ;;
    --rollback-and-reapply)
        REAPPLY=1
        shift
        ;;
    --save-tarballs)
        SAVE_TARBALLS=1
        shift
        ;;
    *)
        break
        ;;
  esac
done


DIR="$1"
shift

if ! git diff-index --quiet HEAD --; then
    set +x
    echo Refusing to run in-place upgrade test with dirty git state.
    echo "(The test makes local modifications.)"
    exit 1
fi

make parsers

./tests/inplace-testing/make-and-prep.sh "$DIR" "$@"

if [ $SAVE_TARBALLS = 1 ]; then
    tar cf "$DIR".tar "$DIR"
fi

PORT=12346
edb server -D "$DIR" -P $PORT &
SPID=$!
cleanup() {
    if [ -n "$SPID" ]; then
        kill $SPID
        wait $SPID
    fi
}
trap cleanup EXIT

# Wait for the server to come up and see it is working
edgedb -H localhost -P $PORT --tls-security insecure -b select query 'select count(User)' | grep 2

# Upgrade to the new version
patch -f -p1 < tests/inplace-testing/upgrade.patch
make parsers

# Get the DSN from the debug endpoint
DSN=$(curl -s http://localhost:$PORT/server-info | jq -r '.pg_addr | "postgres:///?user=\(.user)&port=\(.port)&host=\(.host)"')

# Prepare the upgrade, operating against the postgres that the old
# version server is managing
edb server --inplace-upgrade-prepare "$DIR"/upgrade.json --backend-dsn="$DSN"

# Check the server is still working
edgedb -H localhost -P $PORT --tls-security insecure -b select query 'select count(User)' | grep 2

if [ $ROLLBACK = 1 ]; then
    edb server --inplace-upgrade-rollback --backend-dsn="$DSN"

    # Rollback and then run the tests on the old database
    kill $SPID
    wait $SPID
    SPID=
    patch -R -f -p1 < tests/inplace-testing/upgrade.patch
    make parsers
    edb test --data-dir "$DIR" --use-data-dir-dbs -v "$@"
    exit 0
fi

if [ $REAPPLY = 1 ]; then
    # Rollback and then reapply
    edb server --inplace-upgrade-rollback --backend-dsn="$DSN"

    edb server --inplace-upgrade-prepare "$DIR"/upgrade.json --backend-dsn="$DSN"
fi

# Check the server is still working
edgedb -H localhost -P $PORT --tls-security insecure -b select query 'select count(User)' | grep 2

# Kill the old version so we can finalize the upgrade
kill $SPID
wait $SPID
SPID=

if [ $SAVE_TARBALLS = 1 ]; then
    tar cf "$DIR"-prepped.tar "$DIR"
fi

# Try to finalize the upgrade, but inject a failure
if EDGEDB_UPGRADE_FINALIZE_ERROR_INJECTION=main edb server --inplace-upgrade-finalize --data-dir "$DIR"; then
    echo Unexpected upgrade success despite failure injection
    exit 4
fi

# Finalize the upgrade
edb server --inplace-upgrade-finalize --data-dir "$DIR"
if [ $SAVE_TARBALLS = 1 ]; then
    tar cf "$DIR"-cooked.tar "$DIR"
fi

# Test!
edb test --data-dir "$DIR" --use-data-dir-dbs -v "$@"
