##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import asyncio
import getpass
import os

from . import defines
from .exceptions import *
from . import protocol as edgedb_protocol
from .future import create_future


__all__ = ('connect',) + exceptions.__all__


class Connection:
    def __init__(self, protocol, transport, loop):
        self._protocol = protocol
        self._transport = transport
        self._loop = loop

    async def query(self, query, *args):
        return await self._protocol.execute(query, *args)

    async def execute(self, query, *args):
        return await self._protocol.execute_script(query, *args)

    def close(self):
        self._transport.close()


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
        database = os.getenv('EDGEDB_DATABASE')

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

    return Connection(pr, tr, loop)
