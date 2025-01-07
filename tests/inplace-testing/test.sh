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

# Setup the test database
edb inittestdb -D "$DIR" "$@"


if [ "$SAVE_TARBALLS" = 1 ]; then
    tar cf "$DIR".tar "$DIR"
fi


PORT=12346
edb server -D "$DIR" -P $PORT &
SPID=$!
stop_server() {
    kill $SPID
    wait $SPID
    SPID=
}
cleanup() {
    if [ -n "$SPID" ]; then
        stop_server
    fi
}
trap cleanup EXIT

EDGEDB="edgedb -H localhost -P $PORT --tls-security insecure --wait-until-available 120sec"

# Wait for the server to come up and see it is working
$EDGEDB -b select query 'select count(User)' | grep 2

# Block DDL
$EDGEDB query 'configure instance set force_database_error := $${"type": "AvailabilityError", "message": "DDL is disabled due to in-place upgrade.", "_scopes": ["ddl"]}$$;'

if $EDGEDB query 'create empty branch asdf'; then
    echo Unexpected DDL success despite blocking it
    exit 4
fi

# Prepare the upgrades
EDGEDB_PORT=$PORT EDGEDB_CLIENT_TLS_SECURITY=insecure python3 tests/inplace-testing/prep-upgrades.py > "${DIR}/upgrade.json"

# Upgrade to the new version
patch -f -p1 < tests/inplace-testing/upgrade.patch
make parsers

# Get the DSN from the debug endpoint
DSN=$(curl -s http://localhost:$PORT/server-info | jq -r '.pg_addr.dsn')

# Prepare the upgrade, operating against the postgres that the old
# version server is managing
edb server --inplace-upgrade-prepare "$DIR"/upgrade.json --backend-dsn="$DSN"

# Check the server is still working
$EDGEDB -b select query 'select count(User)' | grep 2

if [ "$ROLLBACK" = 1 ]; then
    # Inject a failure into our first attempt to rollback
    if EDGEDB_UPGRADE_ROLLBACK_ERROR_INJECTION=main edb server --inplace-upgrade-rollback --backend-dsn="$DSN"; then
        echo Unexpected rollback success despite failure injection
        exit 4
    fi

    # Second try should work
    edb server --inplace-upgrade-rollback --backend-dsn="$DSN"
    $EDGEDB query 'configure instance reset force_database_error'

    # Rollback and then run the tests on the old database
    stop_server
    patch -R -f -p1 < tests/inplace-testing/upgrade.patch
    make parsers
    edb test --data-dir "$DIR" --use-data-dir-dbs -v "$@"
    exit 0
fi

if [ "$REAPPLY" = 1 ]; then
    # Rollback and then reapply
    edb server --inplace-upgrade-rollback --backend-dsn="$DSN"

    edb server --inplace-upgrade-prepare "$DIR"/upgrade.json --backend-dsn="$DSN"
fi

# Check the server is still working
$EDGEDB -b select query 'select count(User)' | grep 2

# Kill the old version so we can finalize the upgrade
stop_server

if [ "$SAVE_TARBALLS" = 1 ]; then
    tar cf "$DIR"-prepped.tar "$DIR"
fi

# Try to finalize the upgrade, but inject a failure
if EDGEDB_UPGRADE_FINALIZE_ERROR_INJECTION=main edb server --inplace-upgrade-finalize --data-dir "$DIR"; then
    echo Unexpected upgrade success despite failure injection
    exit 4
fi

# Try doing a rollback. It should fail, because of the partially
# succesful finalization.
if edb server --inplace-upgrade-rollback --data-dir "$DIR"; then
    echo Unexpected upgrade success
    exit 5
fi

# Finalize the upgrade
edb server --inplace-upgrade-finalize --data-dir "$DIR"
if [ "$SAVE_TARBALLS" = 1 ]; then
    tar cf "$DIR"-cooked.tar "$DIR"
fi

# Start the server again so we can reenable DDL
edb server -D "$DIR" -P $PORT &
SPID=$!
if $EDGEDB query 'create empty branch asdf'; then
    echo Unexpected DDL success despite blocking it
    exit 6
fi
$EDGEDB query 'configure instance reset force_database_error'
stop_server
if [ "$SAVE_TARBALLS" = 1 ]; then
    tar cf "$DIR"-cooked2.tar "$DIR"
fi


# Test!
edb test --data-dir "$DIR" --use-data-dir-dbs -v "$@"
