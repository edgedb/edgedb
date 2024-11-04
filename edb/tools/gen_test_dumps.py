#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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
from typing import Any, Dict

import asyncio
import os
import pathlib
import shutil
import sys
import tempfile
import unittest

import click

from edb import buildmeta
from edb.server import cluster as edgedb_cluster
from edb.testbase import server as tb
from edb.tools.edb import edbcommands


class TestResult:
    def wasSuccessful(self):
        return True


class TestRunner:
    def __init__(self):
        self.cases = set()

    def run(self, test):
        self.cases.update(tb.get_test_cases([test]))
        return TestResult()


async def execute(
    tests_dir: str,
    conn: Dict[str, Any],
    num_workers: int,
    version: str,
) -> None:
    runner = TestRunner()
    unittest.main(
        module=None,
        argv=["unittest", "discover", "-s", tests_dir],
        testRunner=runner,
        exit=False,
    )

    setup_scripts = tb.get_test_cases_setup(runner.cases)
    dump_cases = {
        db_name: case
        for case, db_name, _ss in setup_scripts
        if getattr(case, "STABLE_DUMP", False)
    }
    await tb.setup_test_cases(list(dump_cases.values()), conn, num_workers)

    dumps_dir = pathlib.Path(tests_dir) / "dumps"
    db_friendly_version = version.split("+", 1)[0]
    db_friendly_version = db_friendly_version.replace("-alpha.", "a")
    db_friendly_version = db_friendly_version.replace("-beta.", "b")
    db_friendly_version = db_friendly_version.replace("-rc.", "rc")
    db_friendly_version = db_friendly_version.replace("-", "_")
    db_friendly_version = db_friendly_version.replace(".", "_")
    for db_name in dump_cases:
        with tempfile.NamedTemporaryFile() as f:
            tb.CLITestCaseMixin.run_cli_on_connection(
                conn, "-d", db_name, "dump", f.name
            )
            db_dumps_dir = dumps_dir / db_name
            db_dumps_dir.mkdir(exist_ok=True)
            dump_p = (db_dumps_dir / db_friendly_version).with_suffix(".dump")
            shutil.copy(f.name, dump_p)
            print(f"Dumped {dump_p}")


def die(msg):
    print(f"FATAL: {msg}", file=sys.stderr)
    sys.exit(1)


@edbcommands.command("gen-test-dumps")
@click.option(
    "-t",
    "--tests-dir",
    type=str,
    default=str(
        pathlib.Path(__file__).parent.parent.parent.resolve() / "tests"
    ),
    help="directory to start dump test discovery from",
)
@click.option(
    "-j",
    "--jobs",
    type=int,
    default=lambda: round((os.cpu_count() or 1) * 0.75),
    help="number of parallel processes to use",
)
def gen_test_dumps(*, jobs, tests_dir):
    if not jobs:
        jobs = os.cpu_count()

    with tempfile.TemporaryDirectory(
        dir="/tmp/", prefix="edb_gen-test-dumps_"
    ) as data_dir:
        asyncio.run(
            _gen_test_dumps(
                tests_dir=tests_dir,
                data_dir=data_dir,
                jobs=jobs,
            ),
        )


async def _gen_test_dumps(*, jobs: int, tests_dir: str, data_dir: str) -> None:
    version = str(buildmeta.get_version())
    cluster = edgedb_cluster.Cluster(pathlib.Path(data_dir), testmode=True)
    print(
        f"Generating test dumps for version {version}"
        f" with a temporary Gel instance in {data_dir}..."
    )

    try:
        await cluster.init()
        await cluster.start(port=0)
        await cluster.trust_local_connections()
    except BaseException:
        raise

    conn = cluster.get_connect_args()
    try:
        await execute(tests_dir, conn, num_workers=jobs, version=version)
    except BaseException:
        raise
    finally:
        cluster.stop()
        cluster.destroy()
