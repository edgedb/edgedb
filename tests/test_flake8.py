##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import edgedb
import flake8  # NoQA
import os
import subprocess
import sys
import unittest


class TestFlake8(unittest.TestCase):

    def test_flake8(self):
        edgepath = list(edgedb.__path__)[0]
        edgepath = os.path.dirname(edgepath)

        try:
            subprocess.run(
                [sys.executable, '-m', 'flake8'],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=edgepath)
        except subprocess.CalledProcessError as ex:
            raise AssertionError(
                f'flake8 validation failed:\n{ex.output.decode()}') from None
