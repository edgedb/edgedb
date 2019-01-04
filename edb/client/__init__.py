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


import asyncio
import getpass
import os
import time

from . import defines
from .exceptions import *  # NOQA
from . import exceptions
from . import protocol as edgedb_protocol
from . import transaction


__all__ = ('connect',) + exceptions.__all__


class Connection:
    def __init__(self, protocol, transport, loop, dbname):
        self._protocol = protocol
        self._transport = transport
        self._loop = loop
        self._top_xact = None
        self._dbname = dbname

    async def list_dbs(self):
        return await self._protocol.list_dbs()

    async def get_pgcon(self):
        return await self._protocol.get_pgcon()

    async def execute(self, query, *args, graphql=False, flags={}):
        return await self._protocol.execute_script(
            query,
            *args,
            graphql=graphql,
            flags=flags)

    def get_last_timings(self):
        return self._protocol._last_timings

    async def close(self):
        self._transport.close()

    def transaction(self, *, isolation='read_committed', readonly=False,
                    deferrable=False):
        """Create a :class:`~transaction.Transaction` object.

        :param isolation: Transaction isolation mode, can be one of:
                          `'serializable'`, `'repeatable_read'`,
                          `'read_committed'`.

        :param readonly: Specifies whether or not this transaction is
                         read-only.

        :param deferrable: Specifies whether or not this transaction is
                           deferrable.
        """
        return transaction.Transaction(self, isolation, readonly, deferrable)


async def connect(*,
                  host=None, port=None,
                  user=None, password=None,
                  database=None,
                  timeout=60,
                  retry_on_failure=False):

    # On env-var -> connection parameter conversion read here:
    # https://www.postgresql.org/docs/current/static/libpq-envars.html
    # Note that env values may be an empty string in cases when
    # the variable is "unset" by setting it to an empty value
    #
    if host is None:
        host = os.getenv('EDGEDB_HOST')
        if not host:
            host = ['/tmp', '/private/tmp', '/run/edgedb', 'localhost']

    if not isinstance(host, list):
        host = [host]

    if port is None:
        port = os.getenv('EDGEDB_PORT')
        if not port:
            port = defines.EDGEDB_PORT

    if user is None:
        user = os.getenv('EDGEDB_USER')
        if not user:
            user = getpass.getuser()

    if password is None:
        password = os.getenv('EDGEDB_PASSWORD')

    if database is None:
        database = os.getenv('EDGEDB_DATABASE', user)

    budget = timeout
    time_between_tries = 0.1
    loop = asyncio.get_running_loop()

    while budget >= 0:
        start = time.monotonic()

        last_ex = None
        for h in host:
            connected = loop.create_future()

            if h.startswith('/'):
                # UNIX socket name
                sname = os.path.join(h, '.s.EDGEDB.{}'.format(port))
                conn = loop.create_unix_connection(
                    lambda: edgedb_protocol.Protocol(
                        sname, connected, user,
                        password, database, loop),
                    sname)
            else:
                conn = loop.create_connection(
                    lambda: edgedb_protocol.Protocol(
                        (h, port), connected, user,
                        password, database, loop),
                    h, port)

            try:
                tr, pr = await asyncio.wait_for(conn, timeout=budget)
            except (OSError, asyncio.TimeoutError) as ex:
                last_ex = ex
            else:
                last_ex = None
                break

        if last_ex is None:
            try:
                await connected
            except BaseException as ex:
                tr.close()
                last_ex = ex
            else:
                break

        if last_ex is not None:
            if retry_on_failure:
                budget -= time.monotonic() - start + time_between_tries
                if budget > 0:
                    await asyncio.sleep(time_between_tries)
            else:
                raise last_ex

    return Connection(pr, tr, loop, database)
