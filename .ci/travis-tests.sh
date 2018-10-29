#!/bin/bash

set -e -x

edb test -j8 --output-format=simple
