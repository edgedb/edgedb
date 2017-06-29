#!/bin/bash

set -e -x

pip install --upgrade pip wheel
pip install --upgrade setuptools
pip install -r requirements.txt
