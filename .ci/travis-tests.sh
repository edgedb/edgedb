#!/bin/bash

set -e -x

export CI_PROJECT_DIR=$TRAVIS_BUILD_DIR
export PIP_CACHE_DIR=$(pwd)/build/pip/
pip --quiet install vex
vex --python=python3 -m test pip install --quiet -U setuptools wheel pip
vex test pip install --quiet -U -r requirements.txt
vex test python setup.py -q build install
cd /tmp
export EDGEDB_MODPATH=$(vex test python -c 'import edgedb; print(next(iter(edgedb.__path__)))')
vex test pytest --cov="${EDGEDB_MODPATH}" -c "${CI_PROJECT_DIR}/setup.cfg" "${CI_PROJECT_DIR}/tests" "${CI_PROJECT_DIR}/edgedb/lang/common/tests"
cd "${CI_PROJECT_DIR}"
find build -type f ! -name '*.pickle' ! -wholename 'build/pip/*' -delete && find build -type d -empty -delete
