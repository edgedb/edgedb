#!/usr/bin/env python3
#
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import argparse
import asyncio
import os.path
import shutil
import sys
import unittest

from edgedb.server import cluster as edgedb_cluster


class TestResult:
    def wasSuccessful(self):
        return True


class TestRunner:
    def __init__(self):
        self.cases = set()

    def _unroll_tests(self, tests):
        result = set()

        for test in tests:
            if isinstance(test, unittest.TestSuite):
                result.update(self._unroll_tests(test._tests))
            else:
                result.add(type(test))

        return result

    def run(self, test):
        self.cases.update(self._unroll_tests([test]))
        return TestResult()


async def populate_data(cluster, cases):
    conn = await cluster.connect(database='edgedb0', user='edgedb')

    for case in cases:
        if not hasattr(case, 'get_setup_script'):
            continue

        setup_script = case.get_setup_script()
        if not setup_script:
            continue

        if case.__name__.startswith('TestEdgeQL'):
            dbname = case.__name__[len('TestEdgeQL'):]
        elif case.__name__.startswith('Test'):
            dbname = case.__name__[len('Test'):]
        else:
            dbname = case.__name__

        dbname = dbname.lower()
        print(f'CREATE DATABASE {dbname}')
        await conn.execute(f'CREATE DATABASE {dbname};')

        dbconn = await cluster.connect(database=dbname, user='edgedb')

        await dbconn.execute(setup_script)

        dbconn.close()

    conn.close()


async def execute(tests_dir, cluster):
    runner = TestRunner()
    unittest.main(
        module=None,
        argv=['unittest', 'discover', '-s', tests_dir],
        testRunner=runner, exit=False)

    await populate_data(cluster, runner.cases)


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

    args = parser.parse_args()

    if args.start_directory:
        testsdir = args.start_directory
    else:
        testsdir = os.path.abspath(os.path.dirname(__file__))

    return testsdir, args.data_dir


def main():
    tests_dir, data_dir = parse_connect_args()

    if os.path.exists(data_dir):
        if not os.path.isdir(data_dir):
            die(f'{data_dir!r} exists and is not a directory')
        if os.listdir(data_dir):
            die(f'{data_dir!r} exists and is not empty')

    cluster = edgedb_cluster.Cluster(data_dir)
    print(f'Bootstrapping test EdgeDB instance in {data_dir}...')

    try:
        cluster.init()
        cluster.start()
    except BaseException:
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        raise

    destroy_cluster = False

    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(execute(tests_dir, cluster))
        print(f'Initialized and populated test EdgeDB instance in {data_dir}')
    except BaseException:
        destroy_cluster = True
        raise
    finally:
        cluster.stop()
        if destroy_cluster:
            cluster.destroy()
        loop.close()


if __name__ == '__main__':
    main()
