##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import asyncio
import contextlib
import enum
import json
import struct
import time
import traceback

from edgedb.lang import edgeql
from edgedb.lang import graphql as graphql_compiler

from edgedb.server import pgsql as backend
from edgedb.server import executor
from edgedb.server import planner

from edgedb.lang.schema import database as s_db
from edgedb.lang.schema import delta as s_delta
from edgedb.lang.schema import deltas as s_deltas

from edgedb.lang.common import debug
from edgedb.lang.common import markup
from edgedb.lang.common import exceptions
from edgedb.lang.common import parsing


msg_header = struct.Struct('!L')


class Timer:
    __slots__ = ('parse_eql', 'compile_eql_to_ir', 'compile_ir_to_sql',
                 'graphql_translation', 'execution')

    def __init__(self):
        for attr in self.__slots__:
            setattr(self, attr, 0)

    @contextlib.contextmanager
    def timeit(self, name):
        start = time.monotonic()
        try:
            yield
        finally:
            delta = time.monotonic() - start
            prev = getattr(self, name)
            setattr(self, name, prev + delta)

    def as_dict(self):
        return {k: getattr(self, k) for k in self.__slots__}


class ConnectionState(enum.Enum):
    NOT_CONNECTED = 0
    NEW = 1
    READY = 2


class ProtocolError(Exception):
    pass


def is_ddl(plan):
    return isinstance(plan, s_delta.Command) and \
        not isinstance(plan, s_db.DatabaseCommand) and \
        not isinstance(plan, s_deltas.DeltaCommand)


class Protocol(asyncio.Protocol):
    def __init__(self, pg_cluster, loop):
        self._pg_cluster = pg_cluster
        self._loop = loop
        self.pgconn = None
        self.state = ConnectionState.NOT_CONNECTED
        self.transactions = []
        self.buffer = bytearray()

    def connection_made(self, transport):
        self.transport = transport
        self.state = ConnectionState.NEW

    def connection_lost(self, exc):
        self.transport.close()
        if self.pgconn is not None:
            self.pgconn.terminate()

    def data_received(self, data):
        self.buffer.extend(data)
        buf_len = len(self.buffer)
        header_size = msg_header.size
        if buf_len > header_size:
            msg_len, = msg_header.unpack(self.buffer[:header_size])
            if buf_len >= header_size + msg_len:
                msg = self.buffer[header_size:header_size + msg_len]
                self.buffer = self.buffer[header_size + msg_len:]
                msg = json.loads(msg.decode('utf-8'))
                self._loop.call_soon(self.process_message, msg)

    def process_message(self, message):
        if message['__type__'] == 'init':
            database = message.get('database')
            user = message.get('user')

            if not database or not user:
                raise ProtocolError('invalid startup packet')

            fut = self._loop.create_task(
                self._pg_cluster.connect(
                    database=database, user=user, loop=self._loop))

            fut.add_done_callback(self._on_pg_connect)

        elif message['__type__'] == 'query':
            if self.state != ConnectionState.READY:
                raise ProtocolError('unexpected message: query')

            query = message.get('query')
            if not query:
                raise ProtocolError('invalid query message')

            fut = self._loop.create_task(self._run_query(query))
            fut.add_done_callback(self._on_query_done)

        elif message['__type__'] == 'gql_query':
            if self.state != ConnectionState.READY:
                raise ProtocolError('unexpected message: query')

            query = message.get('query')
            if not query:
                raise ProtocolError('invalid graphql query message')

            fut = self._loop.create_task(self._run_query(query))
            fut.add_done_callback(self._on_query_done)

        elif message['__type__'] == 'script':
            if self.state != ConnectionState.READY:
                raise ProtocolError('unexpected message: script')

            script = message.get('script')
            if not script:
                raise ProtocolError('invalid script message')

            fut = self._loop.create_task(
                self._run_script(script, graphql=message.get('__graphql__'),
                                 flags=message.get('__flags__')))
            fut.add_done_callback(self._on_script_done)

        elif message['__type__'] == 'list_dbs':
            fut = self._loop.create_task(self._list_dbs())
            fut.add_done_callback(self._on_script_done)

        elif message['__type__'] == 'get_pgcon':
            fut = self._loop.create_task(self._get_pgcon())
            fut.add_done_callback(self._on_script_done)

    def send_message(self, msg):
        msg = json.dumps(msg).encode('utf-8')
        self.transport.write(msg_header.pack(len(msg)) + msg)

    def send_error(self, err):
        try:
            srcctx = exceptions.get_context(err, parsing.ParserContext)
        except LookupError:
            srcctx = None

        try:
            hintctx = exceptions.get_context(
                err, exceptions.DefaultExceptionContext)
        except LookupError:
            hintctx = None

        if debug.flags.server:
            debug.header('Error')
            debug.dump(err)

        self.send_message({
            '__type__': 'error',
            'data': {
                'C': getattr(err, 'code', 0),
                'M': str(err),
                'D': hintctx.details if hintctx is not None else None,
                'H': hintctx.hint if hintctx is not None else None,
                'P': (srcctx.start.pointer
                      if srcctx is not None and
                      srcctx.start is not None else None),
                'p': (srcctx.end.pointer
                      if srcctx is not None and
                      srcctx.end is not None else None),
                'Q': markup.dumps(srcctx) if srcctx is not None else None,
                'T': traceback.format_tb(err.__traceback__),
            }
        })

    async def _get_pgcon(self):
        timer = Timer()

        with timer.timeit('execution'):
            result = self._pg_cluster.get_connection_spec()

        return result, timer.as_dict()

    async def _list_dbs(self):
        timer = Timer()

        with timer.timeit('execution'):
            result = await self.pgconn.fetch('''
                SELECT d.datname
                    FROM pg_database d
                    INNER JOIN pg_shdescription c ON c.objoid = d.oid
                WHERE
                    d.datistemplate = false AND
                    substr(c.description, 1, 4) = '$CMR';
            ''')

        result = [r['datname'] for r in result]
        return result, timer.as_dict()

    async def _run_script(self, script, *, graphql=False, flags={}):
        timer = Timer()

        if graphql:
            with timer.timeit('graphql_translation'):
                modules = {
                    m.name for m in
                    self.backend.schema.get_modules()
                } - {'schema', 'graphql'}
                script = graphql_compiler.translate(
                    self.backend.schema, script,
                    variables={},
                    modules=modules) + ';'

        with timer.timeit('parse_eql'):
            statements = edgeql.parse_block(script)

        results = []

        for statement in statements:
            plan = planner.plan_statement(
                statement, self.backend, flags, timer=timer)

            with timer.timeit('execution'):
                result = await executor.execute_plan(plan, self)

            if result is not None and isinstance(result, list):
                loaded = []
                for row in result:
                    if isinstance(row, str):
                        # JSON result
                        row = json.loads(row)
                    loaded.append(row)
                result = loaded
            results.append(result)

        return results, timer.as_dict()

    def _on_pg_connect(self, fut):
        try:
            self.pgconn = fut.result()
        except asyncio.CancelledError:
            return
        except Exception as e:
            self.send_error(e)
            return

        fut = self._loop.create_task(backend.open_database(self.pgconn))

        fut.add_done_callback(self._on_edge_connect)

    def _on_edge_connect(self, fut):
        try:
            self.backend = fut.result()
        except asyncio.CancelledError:
            return
        except Exception as e:
            self.send_error(e)
            return

        self.state = ConnectionState.READY

        self.send_message({'__type__': 'authresult', 'result': 'OK'})

    def _on_script_done(self, fut):
        try:
            result, timings = fut.result()
        except asyncio.CancelledError:
            return
        except Exception as e:
            self.send_error(e)
            return

        self.state = ConnectionState.READY

        self.send_message({'__type__': 'result', 'result': result,
                           'timings': timings})
