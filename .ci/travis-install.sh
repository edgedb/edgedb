#!/bin/bash

set -e -x

pip install --quiet -U setuptools wheel pip
pip download --dest=/tmp/deps .[test,docs]
# There is a transitive dependency on wheel through psutil,
# but it is specified in pyproject.toml, and `pip download`
# DOES NOT pick it up, but fails on `pip install` nonetheless.
pip download --dest=/tmp/deps wheel
pip install -U --no-index --find-links=/tmp/deps /tmp/deps/*

git clone https://github.com/edgedb/edgedb-python.git edgedb
cd edgedb
git submodule update --init --depth 50
pip install --verbose -e .
cd ../

pip install --no-deps --verbose -e .
