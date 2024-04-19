#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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

from __future__ import annotations
from typing import Callable, List

import pathlib
import tempfile
import unittest
import unittest.mock

from edb.tools import profiling


class FakeAtexit:
    def __init__(self) -> None:
        self.registered: List[Callable[[], None]] = []

    def register(self, callback: Callable[[], None]) -> None:
        self.registered.append(callback)


def regular_function(arg):
    return hash(arg)


class ProfilingTestCase(unittest.TestCase):
    def test_tools_profiling_basic(self) -> None:
        atexit = FakeAtexit()
        with unittest.mock.patch(
            "edb.tools.profiling.profiler.atexit", atexit
        ):
            with tempfile.TemporaryDirectory() as tmpdir:
                self._inner_basic(tmpdir, atexit)

    def _inner_basic(self, dir: str, atexit: FakeAtexit) -> None:
        profiler = profiling.profile(
            dir=dir, prefix="test_", suffix=".ptest", save_every_n_calls=100
        )

        @profiler
        def profiled_function(arg):
            if arg > 1:
                profiled_function(arg - 1)
            else:
                regular_function(arg)

        # populate the profile
        profiled_function(1)
        profiled_function(2)

        self.assertEqual(len(atexit.registered), 1)

        # dump the .ptest file to the temp directory
        atexit.registered[0]()

        path = pathlib.Path(dir)
        ptest_files = list(path.glob("test_*.ptest"))

        self.assertEqual(len(ptest_files), 1)

        # aggregate the results
        out_file = ptest_files[0].with_suffix(".pstats")
        success, failure = profiler.aggregate(
            out_file, sort_by="cumulative", quiet=True
        )

        self.assertEqual(success, 1)
        self.assertEqual(failure, 0)

        with out_file.open() as out:
            out_contents = out.read()
            self.assertIn("profiled_function", out_contents)
            self.assertIn("regular_function", out_contents)
