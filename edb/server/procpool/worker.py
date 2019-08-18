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

import argparse
import asyncio
import importlib
import base64
import os
import pickle
import signal
import traceback

import uvloop

from edb.common import debug
from edb.common import devmode
from edb.common import markup

from . import amsg


def load_class(cls_name):
    mod_name, _, cls_name = cls_name.rpartition('.')
    mod = importlib.import_module(mod_name)
    cls = getattr(mod, cls_name)
    return cls


async def worker(cls, cls_args, sockname):
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, on_terminate_worker)

    con = await amsg.worker_connect(sockname)
    try:
        worker = cls(*cls_args)

        while True:
            try:
                req = await con.next_request()
            except amsg.PoolClosedError:
                os._exit(0)

            try:
                methname, args = pickle.loads(req)
                meth = getattr(worker, methname)
            except Exception as ex:
                prepare_exception(ex)
                if debug.flags.server:
                    markup.dump(ex)
                data = (
                    1,
                    ex,
                    traceback.format_exc()
                )
            else:
                try:
                    res = await meth(*args)
                    data = (0, res)
                except Exception as ex:
                    prepare_exception(ex)
                    if debug.flags.server:
                        markup.dump(ex)
                    data = (
                        1,
                        ex,
                        traceback.format_exc()
                    )

            try:
                pickled = pickle.dumps(data)
            except Exception as ex:
                ex_tb = traceback.format_exc()
                ex_str = f'{ex}:\n\n{ex_tb}'
                pickled = pickle.dumps((2, ex_str))

            await con.reply(pickled)
    finally:
        con.abort()


def on_terminate_worker():
    # sys.exit() might not do it, apparently.
    os._exit(-1)


def run_worker(cls, cls_args, sockname):
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    with devmode.CoverageConfig.enable_coverage_if_requested():
        asyncio.run(worker(cls, cls_args, sockname))


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cls-name')
    parser.add_argument('--cls-args')
    parser.add_argument('--sockname')
    args = parser.parse_args()

    cls = load_class(args.cls_name)
    cls_args = pickle.loads(base64.b64decode(args.cls_args))

    try:
        run_worker(cls, cls_args, args.sockname)
    except amsg.PoolClosedError:
        exit(0)


if __name__ == '__main__':
    main()
