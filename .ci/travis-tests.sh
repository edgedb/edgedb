#!/bin/bash

set -e -x

export CI_PROJECT_DIR=$TRAVIS_BUILD_DIR
export PIP_CACHE_DIR=$(pwd)/build/pip/
pip --quiet install vex
vex --python=python3 -m test pip install --quiet -U setuptools wheel pip
vex test pip install .
vex test pip install --quiet -U -r .ci/requirements.txt
cd /tmp
export EDGEDB_MODPATH=$(vex test python -c \
    'import edb; print(next(iter(edb.__path__)))')
vex test edb test -j8 "${CI_PROJECT_DIR}/tests"
cd "${CI_PROJECT_DIR}"
find build -type f ! -name '*.pickle' ! -wholename 'build/pip/*' -delete && \
    find build -type d -empty -delete
