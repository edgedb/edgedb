#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present Fantix King
# https://gist.github.com/fantix/c2ddb24b636fb132093a958b08b43665
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

import asyncio
import io
import os
import ssl
import struct
import enum

from edgedb import scram

from edb.common import debug
from edb.testbase import server as tb


DEBUG = debug.flags.server_proto
PID = os.getpid()


def write_string(buf: io.BytesIO, string: str):
    buf.write(string.encode("utf-8"))
    buf.write(b"\x00")


class Message:
    msg_type: bytes = NotImplemented

    def _serialize_type(self, buf):
        buf.write(self.msg_type)

    def _serialize(self, buf):
        pass

    def serialize(self):
        buf = io.BytesIO()

        self._serialize_type(buf)
        type_size = buf.tell()

        buf.write(b"\x00" * 4)
        self._serialize(buf)
        full_size = buf.tell()

        buf.seek(type_size)
        buf.write(struct.pack("!I", full_size - type_size))

        return buf.getvalue()

    def __bytes__(self):
        rv = self.serialize()
        if DEBUG:
            print(PID, "  >", self)
        return rv

    @classmethod
    def deserialize(cls, buf):
        rv = cls()
        rv._deserialize(buf)
        return rv

    def _deserialize(self, buf):
        pass

    def __repr__(self):
        return f"{self.__class__.__name__}({self.values_repr()})"

    def values_repr(self):
        return ", ".join(f"{k}={v!r}" for k, v in self.__dict__.items())

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class ResponseMessage(Message):
    pass


class StartupMessage(Message):
    def __init__(self, user, database=None):
        self.user = user
        self.database = database

    def _serialize_type(self, buf):
        pass

    def _serialize(self, buf):
        buf.write(struct.pack("!HH", 3, 0))
        write_string(buf, "user")
        write_string(buf, self.user)
        if self.database is not None:
            write_string(buf, "database")
            write_string(buf, self.database)
        buf.write(b"\x00")


class Authentication(ResponseMessage):
    msg_type = b"R"

    @classmethod
    def deserialize(self, buf):
        rv = authentication_by_type[struct.unpack("!I", buf[:4])[0]]()
        rv._deserialize(buf[4:])
        return rv


class AuthenticationOK(Authentication):
    pass


class AuthenticationSASL(Authentication):
    def _deserialize(self, buf):
        self.mechanisms = bytes(buf).split(b"\0")[:-2]


class AuthenticationSASLContinue(Authentication):
    def _deserialize(self, buf):
        self.data = bytes(buf)


class AuthenticationSASLFinal(Authentication):
    def _deserialize(self, buf):
        self.additional_data = bytes(buf)


class SASLInitialResponse(Message):
    msg_type = b"p"

    def __init__(self, mechanism: bytes, initial_response: bytes):
        self.mechanism = mechanism
        self.initial_response = initial_response

    def _serialize(self, buf):
        buf.write(self.mechanism)
        buf.write(b"\0")
        buf.write(struct.pack("!i", len(self.initial_response)))
        buf.write(self.initial_response)


class SASLResponse(Message):
    msg_type = b"p"

    def __init__(self, data: bytes):
        self.data = data

    def _serialize(self, buf):
        buf.write(self.data)


class ParameterStatus(ResponseMessage):
    msg_type = b"S"

    def __init__(self):
        self.name = self.value = ""

    def _deserialize(self, buf):
        values = bytes(buf).split(b"\x00")
        self.name = values[0].decode("utf-8")
        self.value = values[1].decode("utf-8")


class BackendKeyData(ResponseMessage):
    msg_type = b"K"

    def _deserialize(self, buf):
        self.connocessID = struct.unpack("!i", buf[:4])[0]
        self.secretKey = struct.unpack("!i", buf[4:])[0]


class ReadyForQuery(ResponseMessage):
    msg_type = b"Z"

    class Type(enum.Enum):
        idle = b"I"
        in_trans = b"T"
        err_trans = b"E"

        def __repr__(self):
            return f"{self.__class__.__name__}.{self.name}"

    def _deserialize(self, buf):
        self.status = self.Type(buf[:1])


class Query(Message):
    msg_type = b"Q"

    def __init__(self, query):
        self.query = query

    def _serialize(self, buf):
        write_string(buf, self.query)


class RowDescription(ResponseMessage):
    msg_type = b"T"

    def __init__(self):
        self.fields = []

    def _deserialize(self, buf):
        offset = 2
        for _ in range(struct.unpack("!H", buf[:2])[0]):
            field = {}
            pos = len(buf)
            for pos in range(offset, len(buf)):
                if buf[pos] == 0:
                    break
            field["name"] = bytes(buf[offset:pos]).decode("utf-8")
            field["table_oid"] = struct.unpack("!i", buf[pos + 1 : pos + 5])[0]
            field["attribute_num"] = struct.unpack(
                "!h", buf[pos + 5 : pos + 7]
            )[0]
            field["type_oid"] = struct.unpack("!i", buf[pos + 7 : pos + 11])[0]
            field["type_size"] = struct.unpack("!h", buf[pos + 11 : pos + 13])[
                0
            ]
            field["type_modifier"] = struct.unpack(
                "!i", buf[pos + 13 : pos + 17]
            )[0]
            field["format_code"] = struct.unpack(
                "!h", buf[pos + 17 : pos + 19]
            )[0]
            self.fields.append(field)
            offset = pos + 19


class EmptyQueryResponse(ResponseMessage):
    msg_type = b"I"


class DataRow(ResponseMessage):
    msg_type = b"D"

    def __init__(self):
        self.values = []

    def _deserialize(self, buf):
        offset = 2
        for _ in range(struct.unpack("!h", buf[:2])[0]):
            size = struct.unpack("!i", buf[offset : offset + 4])[0]
            if size < 0:
                self.values.append(None)
            else:
                self.values.append(bytes(buf[offset + 4 : offset + 4 + size]))
            offset += 4 + size


class CommandComplete(ResponseMessage):
    msg_type = b"C"

    def _deserialize(self, buf):
        self.tag = bytes(buf).split(b"\x00")[0].decode("utf-8")


class ErrorResponse(ResponseMessage):
    msg_type = b"E"

    types = {
        b"S": "severity",
        b"V": "severity",
        b"C": "code",
        b"M": "message",
        b"D": "detail",
        b"H": "hint",
        b"P": "position",
        b"p": "internal_position",
        b"q": "internal_query",
        b"W": "where",
        b"s": "schema_name",
        b"t": "table_name",
        b"c": "column_name",
        b"d": "data_type_name",
        b"n": "constraint_name",
        b"F": "file",
        b"L": "line",
        b"R": "routine",
    }

    def _deserialize(self, buf):
        for field in bytes(buf).split(b"\x00")[:-2]:
            attr = self.types.get(field[:1])
            if attr is not None:
                setattr(self, attr, field[1:].decode("utf-8"))


class Parse(Message):
    msg_type = b"P"

    def __init__(self, query, statement_name=""):
        self.query = query
        self.statement_name = statement_name
        self.param_types = []

    def _serialize(self, buf):
        write_string(buf, self.statement_name)
        write_string(buf, self.query)
        buf.write(struct.pack("!h", len(self.param_types)))
        for t in self.param_types:
            buf.write(struct.pack("!i", t))


class ParseComplete(ResponseMessage):
    msg_type = b"1"


class Describe(Message):
    msg_type = b"D"
    target: bytes = NotImplemented

    def __init__(self, name=""):
        self.name = name

    def _serialize(self, buf):
        buf.write(self.target)
        write_string(buf, self.name)


class DescribeStatement(Describe):
    target = b"S"


class ParameterDescription(ResponseMessage):
    msg_type = b"t"

    def __init__(self):
        self.parameters = []

    def _deserialize(self, buf):
        pass


class Bind(Message):
    msg_type = b"B"

    def __init__(self, portal="", statement="", parameters=None):
        self.portal = portal
        self.statement = statement
        if parameters is None:
            parameters = []
        self.parameters = parameters

    def _serialize(self, buf):
        write_string(buf, self.portal)
        write_string(buf, self.statement)
        buf.write(struct.pack("!h", 0))
        buf.write(struct.pack("!h", len(self.parameters)))
        for param in self.parameters:
            param = str(param).encode("utf-8")
            buf.write(struct.pack("!i", len(param)))
            buf.write(param)
        buf.write(struct.pack("!h", 0))


class BindComplete(ResponseMessage):
    msg_type = b"2"


class DescribePortal(Describe):
    target = b"P"


class Execute(Message):
    msg_type = b"E"

    def __init__(self, portal="", limit=0):
        self.portal = portal
        self.limit = limit

    def _serialize(self, buf):
        write_string(buf, self.portal)
        buf.write(struct.pack("!i", self.limit))


class PortalSuspended(ResponseMessage):
    msg_type = b"s"


class Sync(Message):
    msg_type = b"S"


class Flush(Message):
    msg_type = b"H"


class NoticeResponse(ResponseMessage):
    msg_type = b"N"

    def _deserialize(self, buf):
        self.fields = {}
        for field in bytes(buf).split(b"\x00"):
            if field:
                field_type = struct.unpack("!b", field[:1])[0]
                value = field[1:].decode("utf-8")
                self.fields[field_type] = value


class Close(Message):
    msg_type = b"C"

    def __init__(self, name, close_type="S"):
        self.close_type = close_type
        self.name = name

    def _serialize(self, buf):
        buf.write(self.close_type.encode("utf-8"))
        write_string(buf, self.name)


class CloseComplete(ResponseMessage):
    msg_type = b"3"


class NoData(ResponseMessage):
    msg_type = b"n"


class SSLRequest(Message):
    def _serialize_type(self, buf):
        pass

    def _serialize(self, buf):
        buf.write(
            struct.pack(
                "!i",
                80877103,
            )
        )


class CancelRequest(Message):
    def __init__(self, key_data):
        self.key_data = key_data

    def _serialize_type(self, buf):
        pass

    def _serialize(self, buf):
        buf.write(
            struct.pack(
                "!iii",
                80877102,
                self.key_data.processID,
                self.key_data.secretKey,
            )
        )


class Terminate(Message):
    msg_type = b"X"


messages_by_type = dict(
    (cls.msg_type, cls)
    for cls in locals().values()
    if isinstance(cls, type)
    and issubclass(cls, ResponseMessage)
    and cls.msg_type is not None
)

authentication_by_type = {
    0: AuthenticationOK,
    10: AuthenticationSASL,
    11: AuthenticationSASLContinue,
    12: AuthenticationSASLFinal,
}


def deserialize(data):
    buf = memoryview(data)

    while buf:
        msg_type = bytes(buf[:1])
        msg_size = struct.unpack("!I", buf[1:5])[0]
        payload = buf[5 : msg_size + 1]
        cls = messages_by_type.get(msg_type)
        if cls is None:
            if DEBUG:
                print(PID, "<  ", "skipping:", bytes(buf[: msg_size + 1]))
        else:
            rv = cls.deserialize(payload)
            if DEBUG:
                print(PID, "<  ", rv)
            yield rv
        buf = buf[msg_size + 1:]


class PgProtocol(asyncio.Protocol):
    def __init__(self, sslctx, *, server_hostname=None):
        self._transport = None
        self.messages = asyncio.Queue()
        self.ready = asyncio.Future()
        self.sslctx = sslctx
        self.server_hostname = server_hostname

    def connection_made(self, transport):
        self._transport = transport
        self.write(SSLRequest())

    def data_received(self, data):
        if self.ready.done():
            for msg in deserialize(data):
                self.messages.put_nowait(msg)
        else:
            if data == b"S":
                asyncio.ensure_future(self._start_tls())
            elif data == b"N":
                self.ready.set_result(None)
            else:
                self.ready.set_exception(
                    RuntimeError(f"expect S or N, got {data}")
                )
                self._transport.close()

    async def _start_tls(self):
        try:
            if DEBUG:
                print(PID, "START TLS")
            loop = asyncio.get_running_loop()
            self._transport = await loop.start_tls(
                self._transport,
                self,
                self.sslctx,
                server_hostname=self.server_hostname,
            )
        except Exception as ex:
            self.ready.set_exception(ex)
            self._transport.close()
        else:
            self.ready.set_result(None)

    def write(self, *messages):
        self._transport.writelines(map(bytes, messages))

    def connection_lost(self, exc):
        if DEBUG:
            print(PID, "CONNECTION LOST")
        self.messages.put_nowait(None)

    async def read(self, expect=None):
        rv = await asyncio.wait_for(self.messages.get(), 60)
        if expect is not None and not isinstance(rv, expect):
            raise AssertionError(f"expect {expect}, got {rv}")
        return rv

    async def skip_until(self, expect):
        while True:
            msg = await self.read()
            if msg is None:
                raise ConnectionAbortedError
            elif isinstance(msg, expect):
                break

    async def aclose(self):
        self.write(Terminate())
        while (await self.read()) is not None:
            pass


class TestSQLProtocol(tb.DatabaseTestCase):
    conn: PgProtocol

    def setUp(self):
        super().setUp()
        self.loop.run_until_complete(self.asyncSetUp())

    def tearDown(self):
        try:
            self.loop.run_until_complete(self.conn.aclose())
        finally:
            super().tearDown()

    async def asyncSetUp(self):
        conargs = self.get_connect_args()
        ctx = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH,
            cafile=conargs["tls_ca_file"],
        )
        ctx.check_hostname = False
        _, self.conn = await self.loop.create_connection(
            lambda: PgProtocol(ctx, server_hostname=conargs["host"]),
            conargs["host"],
            conargs["port"],
        )
        await self.conn.ready
        self.conn.write(StartupMessage(conargs["user"], self.con.dbname))
        while True:
            msg = await self.conn.read()
            if isinstance(msg, AuthenticationOK):
                break
            elif isinstance(msg, AuthenticationSASL):
                await self.auth_scram(msg)
            else:
                raise RuntimeError(f"unexpected msg: {msg}")
        await self.conn.skip_until(ReadyForQuery)

    async def auth_scram(self, msg: AuthenticationSASL):
        conargs = self.get_connect_args()
        mechanism = b"SCRAM-SHA-256"

        if mechanism not in msg.mechanisms:
            raise RuntimeError(f"{mechanism!r} not in: {msg.mechanisms}")
        client_nonce = scram.generate_nonce()
        first: tuple[str, str] = scram.build_client_first_message(
            client_nonce, conargs["user"]
        )  # type: ignore
        client_first, client_first_bare = first
        self.conn.write(
            SASLInitialResponse(mechanism, client_first.encode("utf-8"))
        )

        msg_continue = await self.conn.read(AuthenticationSASLContinue)
        server_first = msg_continue.data
        server_nonce, salt, itercount = scram.parse_server_first_message(
            server_first
        )
        final: tuple[str, str] = scram.build_client_final_message(
            conargs["password"],
            salt,
            itercount,
            client_first_bare.encode("utf-8"),
            server_first,
            server_nonce,
        )  # type: ignore
        client_final, expected_server_sig = final
        self.conn.write(SASLResponse(client_final.encode("utf-8")))

        msg_final = await self.conn.read(AuthenticationSASLFinal)
        server_final = msg_final.additional_data
        server_sig = scram.parse_server_final_message(server_final)

        if server_sig != expected_server_sig:
            raise RuntimeError(f"server SCRAM proof does not match")

    async def assert_query_results(self, rows):
        for row in rows:
            msg = await self.conn.read(DataRow)
            self.assertEqual(msg.values, row)
        msg = await self.conn.read(CommandComplete)
        self.assertEqual(msg.tag, f"SELECT {len(rows)}")

    async def assert_simple_query_result(self, rows):
        await self.conn.read(RowDescription)
        await self.assert_query_results(rows)

    async def assert_ready_for_query(self, status=ReadyForQuery.Type.idle):
        msg = await self.conn.read(ReadyForQuery)
        self.assertEqual(msg.status, status)

    async def assert_error_response(self, code=None, message_rexp=None):
        msg = await self.conn.read(ErrorResponse)
        if code is not None:
            self.assertEqual(msg.code, code)
        if message_rexp is not None:
            self.assertRegex(msg.message, message_rexp)

    async def test_sql_proto_simple_query_01(self):
        self.conn.write(Query("SELECT 42"))
        await self.assert_simple_query_result([[b"42"]])
        await self.assert_ready_for_query()

    async def test_sql_proto_simple_query_02(self):
        self.conn.write(
            Query("SELECT 42"), Query("SELECT 42"), Query("SELECT 42")
        )
        for _ in range(3):
            await self.assert_simple_query_result([[b"42"]])
            await self.assert_ready_for_query()

    async def test_sql_proto_simple_query_03(self):
        self.conn.write(Query("SELECT 42; SELECT 42; SELECT 42"))
        for _ in range(3):
            await self.assert_simple_query_result([[b"42"]])
        await self.assert_ready_for_query()

    async def test_sql_proto_simple_query_04(self):
        self.conn.write(
            Query("SELECT 42"), Query("SELECT 1/0"), Query("SELECT 42")
        )

        await self.assert_simple_query_result([[b"42"]])
        await self.assert_ready_for_query()

        await self.assert_error_response("22012", "division by zero")
        await self.assert_ready_for_query()

        await self.assert_simple_query_result([[b"42"]])
        await self.assert_ready_for_query()

    async def test_sql_proto_simple_query_05(self):
        self.conn.write(Query("SELECT 42; SELECT 1/0; SELECT 42"))

        await self.assert_simple_query_result([[b"42"]])
        await self.assert_error_response("22012", "division by zero")
        await self.assert_ready_for_query()

    async def test_sql_proto_simple_query_06(self):
        self.conn.write(Query("SELECT 42; SELT 42"))
        await self.assert_error_response("42601", "syntax error")
        await self.assert_ready_for_query()

    async def test_sql_proto_simple_query_07(self):
        self.conn.write(Query(";"))
        await self.conn.read(EmptyQueryResponse)
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_01(self):
        self.conn.write(
            Parse("SELECT 42"),
            Bind(),
            Execute(),
            Sync(),
        )
        await self.conn.read(ParseComplete)
        await self.conn.read(BindComplete)
        await self.assert_query_results([[b"42"]])
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_02(self):
        self.conn.write(
            Parse("SELECT 42"),
            Bind(),
            Execute(),
            Bind(),
            Execute(),
            Bind(),
            Execute(),
            Sync(),
        )
        await self.conn.read(ParseComplete)
        for _ in range(3):
            await self.conn.read(BindComplete)
            await self.assert_query_results([[b"42"]])
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_03(self):
        self.conn.write(
            Parse("SELECT 42"),
            Parse("SELECT 1/0", "err"),
            Bind(),
            Execute(),
            Bind(statement="err"),
            Execute(),
            Bind(),
            Execute(),
            Sync(),
        )
        await self.conn.read(ParseComplete)
        await self.conn.read(ParseComplete)
        await self.conn.read(BindComplete)
        await self.assert_query_results([[b"42"]])
        await self.assert_error_response("22012", "division by zero")
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_04(self):
        self.conn.write(
            Parse("SELECT 42", "s42"),
            Bind(statement="s42"),
            Execute(),
            Parse("SELT 42", "err"),
            Bind(statement="err"),
            Execute(),
            Bind(statement="s42"),
            Execute(),
            Sync(),
        )
        # On Postgres we will receive the commented out messages
        # await self.conn.read(ParseComplete)
        # await self.conn.read(BindComplete)
        # await self.assert_query_results([[b"42"]])
        await self.assert_error_response("42601", "syntax error")
        await self.assert_ready_for_query()

        self.conn.write(
            Bind(statement="s42"),
            Execute(),
            Sync(),
        )
        await self.conn.read(BindComplete)
        await self.assert_query_results([[b"42"]])
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_05(self):
        self.conn.write(
            Parse("SELECT 42"),
            Parse("SELECT 84"),
            Bind(),
            Bind(),
            Execute(),
            Sync(),
        )
        await self.conn.read(ParseComplete)
        await self.conn.read(ParseComplete)
        await self.conn.read(BindComplete)
        await self.conn.read(BindComplete)
        await self.assert_query_results([[b"84"]])
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_06(self):
        self.conn.write(
            Parse("SELECT 426"),  # for backend cache
            Parse("SELECT 426", "s42"),
            Parse("SELECT 846", "s84"),
            Bind("s84", "s84"),
            Bind("s42", "s42"),
            Execute("s84"),
            Execute("s42"),
            Sync(),
        )
        await self.conn.read(ParseComplete)
        await self.conn.read(ParseComplete)
        await self.conn.read(ParseComplete)
        await self.conn.read(BindComplete)
        await self.conn.read(BindComplete)
        await self.assert_query_results([[b"846"]])
        await self.assert_query_results([[b"426"]])
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_07(self):
        self.conn.write(
            Parse("SELECT 42", "stmt7"),
            Parse("SELECT 84", "stmt7"),
            Bind(),
            Execute(),
            Sync(),
        )
        # On Postgres we will also receive:
        # await self.conn.read(ParseComplete)
        await self.assert_error_response(
            "42P05", 'prepared statement "stmt7" already exists'
        )
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_08(self):
        self.conn.write(
            Parse("SELECT 42"),
            Bind("portal8"),
            Bind("portal8"),
            Execute("portal8"),
            Sync(),
        )
        # On Postgres we will also receive:
        # await self.conn.read(ParseComplete)
        # await self.conn.read(BindComplete)
        await self.assert_error_response(
            "42P03", 'cursor "portal8" already exists'
        )
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_09(self):
        self.conn.write(
            Bind(statement="stmt9"),
            Execute(),
            Sync(),
        )
        await self.assert_error_response(
            "26000", 'prepared statement "stmt9" does not exist'
        )
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_10(self):
        self.conn.write(
            Execute("portal10"),
            Sync(),
        )
        await self.assert_error_response(
            "34000", 'cursor "portal10" does not exist'
        )
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_11(self):
        self.conn.write(
            Parse("SELECT 42"),
            Bind(),
            Flush(),
        )
        await self.conn.read(ParseComplete)
        await self.conn.read(BindComplete)
        self.conn.write(
            Execute(),
            Sync(),
        )
        await self.assert_query_results([[b"42"]])
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_12(self):
        self.conn.write(
            Parse("SELECT 42"),
            Flush(),
        )
        await self.conn.read(ParseComplete)
        self.conn.write(
            Bind(),
            Execute(),
            Sync(),
        )
        await self.conn.read(BindComplete)
        await self.assert_query_results([[b"42"]])
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_13(self):
        self.conn.write(
            Parse("SELECT 42"),
            Bind(),
            Sync(),
        )
        await self.conn.read(ParseComplete)
        await self.conn.read(BindComplete)
        await self.assert_ready_for_query()

        self.conn.write(
            Execute(),
            Sync(),
        )
        await self.assert_error_response("34000", 'cursor "" does not exist')
        await self.assert_ready_for_query()

        self.conn.write(
            Bind(),
            Execute(),
            Sync(),
        )
        await self.conn.read(BindComplete)
        await self.assert_query_results([[b"42"]])
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_14(self):
        self.conn.write(
            Parse("SELECT 42"),
            Bind(),
            Execute(),
            Execute(),
            Flush(),
        )
        await self.conn.read(ParseComplete)
        await self.conn.read(BindComplete)
        await self.assert_query_results([[b"42"]])
        await self.assert_query_results([])

        self.conn.write(
            Execute(),
            Sync(),
        )
        await self.assert_query_results([])
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_15(self):
        self.conn.write(
            Parse("SELECT 42"),
            Bind(),
            Execute(limit=1),
            Sync(),
        )
        await self.conn.read(ParseComplete)
        await self.conn.read(BindComplete)
        msg = await self.conn.read(DataRow)
        self.assertEqual(msg.values, [b"42"])
        await self.conn.read(PortalSuspended)
        await self.assert_ready_for_query()

        self.conn.write(
            Bind(),
            Execute(limit=1),
            Flush(),
        )
        await self.conn.read(BindComplete)
        msg = await self.conn.read(DataRow)
        self.assertEqual(msg.values, [b"42"])
        await self.conn.read(PortalSuspended)

    async def test_sql_proto_extended_query_16(self):
        self.conn.write(Parse("SELT 42"))
        await self.assert_error_response("42601", "syntax error")
        self.conn.write(Bind())
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.conn.messages.get(), 0.1)
        self.conn.write(Flush())
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.conn.messages.get(), 0.1)
        self.conn.write(Sync())
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_17(self):
        self.conn.write(
            Parse("SELECT 42", "stmt17"),
            Bind(statement="stmt17"),
            Execute(),
            Close("stmt17"),
            Bind(statement="stmt17"),
            Execute(),
            Sync(),
        )
        # On Postgres we will also receive:
        # await self.conn.read(ParseComplete)
        # await self.conn.read(BindComplete)
        # await self.assert_query_results([[b"42"]])
        # await self.conn.read(CloseComplete)
        await self.assert_error_response(
            "26000", 'prepared statement "stmt17" does not exist'
        )
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_18(self):
        self.conn.write(
            Parse("SELECT 42", "stmt18"),
            Bind(statement="stmt18"),
            Execute(),
            Sync(),
        )
        await self.conn.read(ParseComplete)
        await self.conn.read(BindComplete)
        await self.assert_query_results([[b"42"]])
        await self.assert_ready_for_query()

        self.conn.write(
            Close("stmt18"),
            Flush(),
        )
        await self.conn.read(CloseComplete)

        self.conn.write(
            Bind(statement="stmt18"),
            Execute(),
            Sync(),
        )
        await self.assert_error_response(
            "26000", 'prepared statement "stmt18" does not exist'
        )
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_19(self):
        self.conn.write(
            Parse("SELECT 42"),
            Bind("portal19"),
            Execute("portal19"),
            Close("portal19", close_type="P"),
            Execute("portal19"),
            Sync(),
        )
        # On Postgres we will also receive:
        # await self.conn.read(ParseComplete)
        # await self.conn.read(BindComplete)
        # await self.assert_query_results([[b"42"]])
        # await self.conn.read(CloseComplete)
        await self.assert_error_response(
            "34000", 'cursor "portal19" does not exist'
        )
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_20(self):
        self.conn.write(
            Parse("SELECT 42"),
            Bind("portal20"),
            Flush(),
        )
        await self.conn.read(ParseComplete)
        await self.conn.read(BindComplete)

        self.conn.write(
            Close("portal20", close_type="P"),
            Flush(),
        )
        await self.conn.read(CloseComplete)

        self.conn.write(
            Execute("portal20"),
            Sync(),
        )
        await self.assert_error_response(
            "34000", 'cursor "portal20" does not exist'
        )
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_21(self):
        self.conn.write(
            Parse("SELECT 42", "stmt21"),
            DescribeStatement("stmt21"),
            Sync(),
        )
        await self.conn.read(ParseComplete)
        param_desc = await self.conn.read(ParameterDescription)
        row_desc = await self.conn.read(RowDescription)
        await self.assert_ready_for_query()

        self.conn.write(
            DescribeStatement("stmt21"),
            Sync(),
        )
        self.assertEqual(
            param_desc, await self.conn.read(ParameterDescription)
        )
        self.assertEqual(row_desc, await self.conn.read(RowDescription))
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_22(self):
        self.conn.write(
            Parse("SELECT 42"),
            Bind("portal22"),
            DescribePortal("portal22"),
            Flush(),
        )
        await self.conn.read(ParseComplete)
        await self.conn.read(BindComplete)
        row_desc = await self.conn.read(RowDescription)

        self.conn.write(
            DescribePortal("portal22"),
            Sync(),
        )
        self.assertEqual(row_desc, await self.conn.read(RowDescription))
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_23(self):
        self.conn.write(
            Parse("SELECT $1"),
            Bind(parameters=[42]),
            Execute(),
            Sync(),
        )
        await self.conn.read(ParseComplete)
        await self.conn.read(BindComplete)
        await self.assert_query_results([[b"42"]])
        await self.assert_ready_for_query()

    async def test_sql_proto_extended_query_24(self):
        self.conn.write(
            Parse("SELECT $1"),
            Bind(parameters=[42, "err"]),
            Execute(),
            Sync(),
        )
        await self.conn.read(ParseComplete)
        await self.assert_error_response(
            "08P01", r"supplies 2 parameters.*requires 1"
        )
        await self.assert_ready_for_query()
