#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2017-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import os
import subprocess
import sys
import unittest


# The below files must not have any Python code in them;
# there should be a comment in each of them explaining why.
EMPTY_INIT_FILES = {
    'edb/__init__.py',
    'edb/common/__init__.py',
    'edb/tools/__init__.py',
}


def find_edgedb_root():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestCodeQuality(unittest.TestCase):

    def test_cqa_empty_init(self):
        edgepath = find_edgedb_root()
        for sn in EMPTY_INIT_FILES:
            fn = os.path.join(edgepath, sn)
            if not os.path.exists(fn):
                self.fail(f'not found an empty __init__.py file at {fn}')

            with open(fn, 'rt') as f:
                for line in f:
                    if line.startswith('#') or not line.strip():
                        continue

                    self.fail(
                        f'{fn} must be an empty file (except Python comments)')

    def test_cqa_ruff(self):
        edgepath = find_edgedb_root()

        try:
            import ruff  # NoQA
        except ImportError:
            raise unittest.SkipTest('ruff module is missing')

        for subdir in ['edb', 'tests']:  # ignore any top-level test files
            try:
                subprocess.run(
                    ['ruff', 'check', '.'],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=os.path.join(edgepath, subdir))
            except subprocess.CalledProcessError as ex:
                output = ex.output.decode()
                raise AssertionError(
                    f'ruff validation failed:\n{output}') from None

    def test_cqa_mypy(self):
        edgepath = find_edgedb_root()
        config_path = os.path.join(edgepath, 'pyproject.toml')
        if not os.path.exists(config_path):
            raise RuntimeError('could not locate pyproject.toml file')

        try:
            import mypy  # NoQA
        except ImportError:
            raise unittest.SkipTest('mypy module is missing')

        for subdir in ['edb', 'tests']:  # ignore any top-level test files
            try:
                subprocess.run(
                    [
                        sys.executable,
                        '-m',
                        'mypy',
                        '--config-file',
                        config_path,
                        subdir,
                    ],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=edgepath,
                )
            except subprocess.CalledProcessError as ex:
                output = ex.stdout.decode()
                if ex.stderr:
                    output += '\n\n' + ex.stderr.decode()
                raise AssertionError(
                    f'mypy validation failed:\n{output}') from None

    def test_cqa_rust_clippy(self):
        edgepath = find_edgedb_root()
        config_path = os.path.join(edgepath, 'Cargo.toml')
        if not os.path.exists(config_path):
            raise RuntimeError('could not locate Cargo.toml file')

        try:
            subprocess.run(
                [
                    "cargo",
                    'clippy',
                    '--',
                    '-Dclippy::all',
                ],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=edgepath,
            )
        except subprocess.CalledProcessError as ex:
            output = ex.stdout.decode()
            if ex.stderr:
                output += '\n\n' + ex.stderr.decode()
            raise AssertionError(
                f'clippy validation failed:\n{output}') from None

    def test_cqa_rust_rustfmt(self):
        edgepath = find_edgedb_root()
        config_path = os.path.join(edgepath, 'Cargo.toml')
        if not os.path.exists(config_path):
            raise RuntimeError('could not locate Cargo.toml file')

        try:
            subprocess.run(
                [
                    "cargo",
                    'fmt',
                    '--check',
                ],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=edgepath,
            )
        except subprocess.CalledProcessError as ex:
            output = ex.stdout.decode()
            if ex.stderr:
                output += '\n\n' + ex.stderr.decode()
            raise AssertionError(
                f'rustfmt validation failed:\n{output}') from None
