# mypy: ignore-errors

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

import logging
import multiprocessing.pool
import multiprocessing.process
import multiprocessing.util


_orig_pool_worker_handler = None
_orig_pool_join_exited_workers = None

logger = logging.getLogger(__name__)


class WorkerScope:

    def __init__(self, initializer, destructor):
        self.initializer = initializer
        self.destructor = destructor

    def __call__(self, *args, **kwargs):
        # Make multiprocessing.Pool happy
        return self.initializer(*args, **kwargs)


def multiprocessing_pool_worker(
    inqueue, outqueue, initializer=None, *args, **kwargs
):
    destructor = None
    if isinstance(initializer, WorkerScope):
        destructor = initializer.destructor

    # This function is executed in the context of a spawned
    # worker process, so the pool.worker() function is the
    # original unpatched version.
    try:
        multiprocessing.pool.worker(
            inqueue, outqueue, initializer, *args, **kwargs)
    except KeyboardInterrupt:
        # Try to exit with less noise when ctrl+c is pressed
        return

    if destructor is not None:
        destructor()


def multiprocessing_worker_handler(*args):
    _orig_pool_worker_handler(*args)

    if len(args) == 1:
        # In some pythons this is a static method with
        # a single argument...
        workers = args[0]._pool
    else:
        # ... and in others it's a staticmethod or a classmethod taking
        # 12-14 positional arguments.
        for arg in args:
            if (isinstance(arg, list) and arg
                    and isinstance(
                        arg[0],
                        multiprocessing.process.BaseProcess)):
                workers = arg
                break
        else:
            logger.error(
                'unable to patch multiprocessing.Pool._handle_workers')
            return

    for worker_process in workers:
        # Give workers ample time to shutdown, and
        # if they don't, the pool will terminate them.
        worker_process.join(timeout=10)


def join_exited_workers(pool):
    # Our use case shouldn't have workers exiting really, so we skip
    # doing the joins so that we can detect crashes ourselves in the
    # test runner.x
    pass


def patch_multiprocessing(debug: bool):
    global _orig_pool_worker
    global _orig_pool_worker_handler
    global _orig_pool_join_exited_workers

    if debug:
        multiprocessing.util.log_to_stderr(logging.DEBUG)

    # A "fork" without "exec" is broken on macOS since 10.14:
    # https://www.wefearchange.org/2018/11/forkmacos.rst.html
    # Since there is no apparent benefit of using fork for
    # the test workers, use the "spawn" method on all platforms.
    multiprocessing.set_start_method('spawn')

    # Add the ability to do clean shutdown of the worker.
    multiprocessing.pool.worker = multiprocessing_pool_worker

    # Allow workers some time to shut down gracefully.
    _orig_pool_worker_handler = multiprocessing.pool.Pool._handle_workers
    multiprocessing.pool.Pool._handle_workers = multiprocessing_worker_handler

    _orig_pool_join_exited_workers = (
        multiprocessing.pool.Pool._join_exited_workers)
    multiprocessing.pool.Pool._join_exited_workers = join_exited_workers
