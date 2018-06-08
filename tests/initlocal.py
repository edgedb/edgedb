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


import argparse
import os.path
import shutil
import sys
import unittest

from edb.server import cluster as edgedb_cluster
from edb.server import _testbase as tb


class TestResult:
    def wasSuccessful(self):
        return True


class TestRunner:
    def __init__(self):
        self.cases = set()

    def run(self, test):
        self.cases.update(tb.get_test_cases([test]))
        return TestResult()


def execute(tests_dir, conns):
    runner = TestRunner()
    unittest.main(
        module=None,
        argv=['unittest', 'discover', '-s', tests_dir],
        testRunner=runner, exit=False)

    tb.setup_test_cases(runner.cases, conns)


def die(msg):
    print(f'FATAL: {msg}', file=sys.stderr)
    sys.exit(1)


def parse_connect_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-D', '--data-dir', type=str,
        default=os.path.join(os.environ['HOME'], '.edgedb'),
        help='database cluster directory (default ~/.edgedb)')
    parser.add_argument(
        '-s', '--start-directory', type=str,
        help='directory to start test discovery from')
    parser.add_argument(
        '-j', '--jobs', type=int,
        help='number of parallel processes to use (defaults to CPU count)')

    args = parser.parse_args()

    if args.start_directory:
        testsdir = args.start_directory
    else:
        testsdir = os.path.abspath(os.path.dirname(__file__))

    return testsdir, args.data_dir, args.jobs


def main():
    tests_dir, data_dir, jobs = parse_connect_args()

    if os.path.exists(data_dir):
        if not os.path.isdir(data_dir):
            die(f'{data_dir!r} exists and is not a directory')
        if os.listdir(data_dir):
            die(f'{data_dir!r} exists and is not empty')

    if not jobs:
        jobs = os.cpu_count()

    cluster = edgedb_cluster.Cluster(data_dir)
    print(f'Bootstrapping test EdgeDB instance in {data_dir}...')

    try:
        cluster.init()
        cluster.start(port='dynamic', timezone='UTC')
    except BaseException:
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        raise

    servers, conns = tb.start_worker_servers(cluster, num_workers=jobs)
    destroy_cluster = False

    try:
        execute(tests_dir, conns)
        print(f'Initialized and populated test EdgeDB instance in {data_dir}')
    except BaseException:
        destroy_cluster = True
        raise
    finally:
        tb.shutdown_worker_servers(servers, destroy=destroy_cluster)


if __name__ == '__main__':
    main()
