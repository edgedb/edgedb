#!/bin/bash -ex

DIR="$1"
shift

if ! git diff-index --quiet HEAD --; then
    set +x
    echo Refusing to run in-place upgrade test with dirty git state.
    echo "(The test makes local modifications.)"
    exit 1
fi

./tests/inplace-testing/make-and-prep.sh "$DIR" "$@"

tar cf "$DIR".tar "$DIR"

patch -f -p1 < tests/inplace-testing/upgrade.patch

edb server --bootstrap-only --inplace-upgrade "$DIR"/upgrade.json --data-dir "$DIR"

tar cf "$DIR"-cooked.tar "$DIR"

edb test --data-dir "$DIR" --use-data-dir-dbs -v
