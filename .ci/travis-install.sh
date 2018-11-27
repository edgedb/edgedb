#!/bin/bash

set -e -x

git clone https://github.com/edgedb/edgedb-python.git edgedb
cd edgedb
git submodule update --init --depth 1
pip install --verbose -e .
cd ../

pip install --quiet -U setuptools wheel pip
pip download --dest=/tmp/deps .[test,docs]
pip install -U --no-index --find-links=/tmp/deps /tmp/deps/*
pip install --verbose -e .
