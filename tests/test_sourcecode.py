##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import subprocess
import sys
import unittest


def find_edgedb_root():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestFlake8(unittest.TestCase):

    def test_flake8(self):
        edgepath = find_edgedb_root()
        config_path = os.path.join(edgepath, '.flake8')
        if not os.path.exists(config_path):
            raise RuntimeError('could not locate .flake8 file')

        try:
            import flake8  # NoQA
        except ImportError:
            raise unittest.SkipTest('flake8 moudule is missing')

        for subdir in ['edgedb', 'tests']:  # ignore any top-level test files
            try:
                subprocess.run(
                    [sys.executable, '-m', 'flake8', '--config', config_path],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=os.path.join(edgepath, subdir))
            except subprocess.CalledProcessError as ex:
                output = ex.output.decode()
                raise AssertionError(
                    f'flake8 validation failed:\n{output}') from None
