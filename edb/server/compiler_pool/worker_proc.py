#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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
import gc
import os
import pickle
import signal
import time
import traceback

from edb.common import debug
from edb.common import devmode
from edb.common import markup
from edb.edgeql import parser as ql_parser

from . import amsg


# "created continuously" means the interval between two consecutive spawns
# is less than NUM_SPAWNS_RESET_INTERVAL seconds.
NUM_SPAWNS_RESET_INTERVAL = 1


def worker(sockname, version_serial, get_handler):
    con = amsg.WorkerConnection(sockname, version_serial)
    try:
        for req_id, req in con.iter_request():
            try:
                methname, args = pickle.loads(req)
                meth = get_handler(methname)
            except Exception as ex:
                prepare_exception(ex)
                if debug.flags.server:
                    markup.dump(ex)
                data = (1, ex, traceback.format_exc())
            else:
                try:
                    res = meth(*args)
                    data = (0, res)
                except Exception as ex:
                    prepare_exception(ex)
                    if debug.flags.server:
                        markup.dump(ex)
                    data = (1, ex, traceback.format_exc())

            try:
                pickled = pickle.dumps(data, -1)
            except Exception as ex:
                ex_tb = traceback.format_exc()
                ex_str = f"{ex}:\n\n{ex_tb}"
                pickled = pickle.dumps((2, ex_str), -1)

            con.reply(req_id, pickled)
    finally:
        con.abort()


def run_worker(sockname, version_serial, get_handler):
    with devmode.CoverageConfig.enable_coverage_if_requested():
        worker(sockname, version_serial, get_handler)


def prepare_exception(ex):
    clear_exception_frames(ex)
    if ex.__traceback__ is not None:
        ex.__traceback__ = ex.__traceback__.tb_next


def clear_exception_frames(er):
    def _clear_exception_frames(er, visited):
        if er in visited:
            return er
        visited.add(er)

        traceback.clear_frames(er.__traceback__)

        if er.__cause__ is not None:
            er.__cause__ = _clear_exception_frames(er.__cause__, visited)
        if er.__context__ is not None:
            er.__context__ = _clear_exception_frames(er.__context__, visited)

        return er

    visited = set()
    _clear_exception_frames(er, visited)


def listen_for_debugger():
    if debug.flags.pydebug_listen:
        import debugpy
        debugpy.listen(38781)


def main(get_handler):
    parser = argparse.ArgumentParser()
    parser.add_argument("--sockname")
    parser.add_argument("--numproc")
    parser.add_argument("--version-serial", type=int)
    args = parser.parse_args()

    ql_parser.preload_spec()
    gc.freeze()

    listen_for_debugger()

    if args.numproc is None:
        # Run a single worker process
        run_worker(args.sockname, args.version_serial, get_handler)
        return

    numproc = int(args.numproc)
    assert numproc >= 1

    # Abort the template process if more than `max_worker_spawns`
    # new workers are created continuously - it probably means the
    # worker cannot start correctly.
    max_worker_spawns = numproc * 2

    children = set()
    continuous_num_spawns = 0

    for _ in range(int(args.numproc)):
        # spawn initial workers
        if pid := os.fork():
            # main process
            children.add(pid)
            continuous_num_spawns += 1
        else:
            # child process
            break
    else:
        # main process - redirect SIGTERM to SystemExit and wait for children
        signal.signal(signal.SIGTERM, lambda *_: exit(os.EX_OK))
        last_spawn_timestamp = time.monotonic()

        try:
            while children:
                pid, status = os.wait()
                children.remove(pid)
                ec = os.waitstatus_to_exitcode(status)
                if ec > 0 or -ec not in {0, signal.SIGINT}:
                    # restart the child process if killed or ending abnormally,
                    # unless we tried too many times continuously
                    now = time.monotonic()
                    if now - last_spawn_timestamp > NUM_SPAWNS_RESET_INTERVAL:
                        continuous_num_spawns = 0
                    last_spawn_timestamp = now
                    continuous_num_spawns += 1
                    if continuous_num_spawns > max_worker_spawns:
                        # GOTCHA: we shouldn't return here because we need the
                        # exception handler below to clean up the workers
                        exit(os.EX_UNAVAILABLE)

                    if pid := os.fork():
                        # main process
                        children.add(pid)
                    else:
                        # child process
                        break
            else:
                # main process - all children ended normally
                return
        except BaseException as e:  # includes SystemExit and KeyboardInterrupt
            # main process - kill and wait for the remaining workers to exit
            try:
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                for pid in children:
                    try:
                        os.kill(pid, signal.SIGTERM)
                    except OSError:
                        pass
                try:
                    while children:
                        pid, status = os.wait()
                        children.discard(pid)
                except OSError:
                    pass
            finally:
                raise e

    # child process - clear the SIGTERM handler for potential Rust impl
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    run_worker(args.sockname, args.version_serial, get_handler)
