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
from typing import List

import asyncio
import os.path
import pathlib
import shutil
import sys
import unittest

import click

from edb.common import devmode
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


async def execute(tests_dir, conn, num_workers, include):
    runner = TestRunner()
    include = [x for pat in include for x in ['-k', pat]]
    unittest.main(
        module=None,
        argv=['unittest', 'discover', '-s', tests_dir, *include],
        testRunner=runner, exit=False)

    await tb.setup_test_cases(
        runner.cases, conn, num_workers, skip_empty_databases=True
    )


def die(msg):
    print(f'FATAL: {msg}', file=sys.stderr)
    sys.exit(1)


@edbcommands.command()
@click.option(
    '-D', '--data-dir',
    type=str,
    default=str(devmode.get_dev_mode_data_dir()),
    help='database cluster directory',
)
@click.option(
    '-t', '--tests-dir', type=str,
    default=str(pathlib.Path(__file__).parent.parent.parent.resolve() /
                'tests'),
    help='directory to start test discovery from')
@click.option('-j', '--jobs', type=int,
              default=lambda: round((os.cpu_count() or 1) * 0.75),
              help='number of parallel processes to use')
@click.option('-k', '--include', type=str, multiple=True, metavar='REGEXP',
              help='only use tests which match the given regular expression')
@click.option('-u', '--update', is_flag=True,
              help='add the tests to the existing db')
def inittestdb(*, data_dir, jobs, tests_dir, include, update):
    if os.path.exists(data_dir):
        if not os.path.isdir(data_dir):
            die(f'{data_dir!r} exists and is not a directory')
        if os.listdir(data_dir) and not update:
            die(f'{data_dir!r} exists and is not empty')

    if not jobs:
        jobs = os.cpu_count()

    asyncio.run(
        _inittestdb(
            jobs=jobs,
            data_dir=data_dir,
            tests_dir=tests_dir,
            include=include,
            update=update,
        ),
    )


async def _inittestdb(
    *,
    jobs: int,
    data_dir: str,
    tests_dir: str,
    include: List[str],
    update: bool,
) -> None:
    cluster = edgedb_cluster.Cluster(pathlib.Path(data_dir), testmode=True)

    try:
        if not update:
            print(f'Bootstrapping test Gel instance in {data_dir}...')
            await cluster.init()
        await cluster.start(port=0)
    except BaseException:
        if not update:
            if os.path.exists(data_dir):
                shutil.rmtree(data_dir)
        raise

    conn = cluster.get_connect_args()
    destroy_cluster = False

    try:
        await execute(tests_dir, conn, num_workers=jobs, include=include)
        print(f'Initialized and populated test Gel instance in {data_dir}')
    except BaseException:
        destroy_cluster = True
        raise
    finally:
        cluster.stop()
        if destroy_cluster:
            cluster.destroy()
