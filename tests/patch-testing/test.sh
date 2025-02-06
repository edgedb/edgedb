#!/bin/bash -ex

while [[ $# -gt 0 ]]; do
  case $1 in
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
    echo Refusing to run patching upgrade test with dirty git state.
    echo "(The test makes local modifications.)"
    exit 1
fi

make parsers

# Setup the test database
edb inittestdb -D "$DIR" "$@"


if [ "$SAVE_TARBALLS" = 1 ]; then
    tar cf "$DIR".tar "$DIR"
fi


# Upgrade to the new version
patch -f -p1 < tests/patch-testing/upgrade.patch
make parsers

edb server --bootstrap-only --data-dir "$DIR"

if [ "$SAVE_TARBALLS" = 1 ]; then
    tar cf "$DIR"-upgraded.tar "$DIR"
fi

# Test!
edb test --data-dir "$DIR" --use-data-dir-dbs -v "$@"
