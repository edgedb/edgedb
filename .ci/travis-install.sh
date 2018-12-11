#!/bin/bash

set -e -x

pip install --quiet -U setuptools wheel pip
pip download --dest=/tmp/deps .[test,docs]
pip install -U --no-index --find-links=/tmp/deps /tmp/deps/*
pip install --verbose -e .
