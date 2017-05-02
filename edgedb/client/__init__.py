##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import asyncio
import getpass
import os

from . import defines
from .exceptions import *  # NOQA
from . import exceptions
from . import protocol as edgedb_protocol
from .future import create_future
from . import transaction


__all__ = ('connect',) + exceptions.__all__


class Connection:
    def __init__(self, protocol, transport, loop, dbname):
        self._protocol = protocol
        self._transport = transport
        self._loop = loop
        self._top_xact = None
        self._dbname = dbname
        self._optimize = False

    def set_optimize(self, flag: bool):
        self._optimize = bool(flag)

    def get_optimize(self):
        return self._optimize

    async def execute(self, query, *args, graphql=False, flags={}):
        return await self._protocol.execute_script(
            query,
            *args,
            graphql=graphql,
            optimize=self._optimize,
            flags=flags)

    def get_last_timings(self):
        return self._protocol._last_timings

    def close(self):
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
                  loop=None,
                  timeout=60):

    if loop is None:
        loop = asyncio.get_event_loop()

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
        database = os.getenv('EDGEDB_DATABASE', 'edgedb')

    last_ex = None
    for h in host:
        connected = create_future(loop)

        if h.startswith('/'):
            # UNIX socket name
            sname = os.path.join(h, '.s.EDGEDB.{}'.format(port))
            conn = loop.create_unix_connection(
                lambda: edgedb_protocol.Protocol(sname, connected, user,
                                                 password, database, loop),
                sname)
        else:
            conn = loop.create_connection(
                lambda: edgedb_protocol.Protocol((h, port), connected, user,
                                                 password, database, loop),
                h, port)

        try:
            tr, pr = await asyncio.wait_for(conn, timeout=timeout, loop=loop)
        except (OSError, asyncio.TimeoutError) as ex:
            last_ex = ex
        else:
            break
    else:

        raise last_ex
    try:
        await connected
    except:
        tr.close()
        raise

    return Connection(pr, tr, loop, database)
