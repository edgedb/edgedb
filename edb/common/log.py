#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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

# DON'T IMPORT asyncio or any package that creates their own logger here,
# or the "tenant" value cannot be injected.
import contextvars
import logging


current_tenant = contextvars.ContextVar("current_tenant", default="-")


class EdgeDBLogger(logging.Logger):

    def makeRecord(
        self,
        name,
        level,
        fn,
        lno,
        msg,
        args,
        exc_info,
        func=None,
        extra=None,
        sinfo=None,
    ):
        # Unlike the standard Logger class, we allow overwriting
        # all attributes of the log record with stuff from *extra*.
        factory = logging.getLogRecordFactory()
        rv = factory(name, level, fn, lno, msg, args, exc_info, func, sinfo)
        rv.__dict__["tenant"] = current_tenant.get()
        if extra is not None:
            rv.__dict__.update(extra)
        return rv


def early_setup():
    logging.setLoggerClass(EdgeDBLogger)
