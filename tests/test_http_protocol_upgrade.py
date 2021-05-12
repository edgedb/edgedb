#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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

import io

from edb.common import binwrapper
from edb.testbase import http as tb
from edb.testbase import protocol
from edb.testbase import server as tb_server
from edb.testbase.protocol import protocol as tb_protocol  # type: ignore


class TestHttpProtocolUpgrade(tb.BaseHttpTest, tb_server.ConnectedTestCase):
    @classmethod
    def get_api_path(cls) -> str:
        return "/"

    def test_http_upgrade_success(self):
        with self.http_con() as con:
            con.request(
                "GET",
                "/server/status/ready",
                headers={"Connection": "upgrade", "Upgrade": "edgedb-binary"},
            )
            data, headers, status = self.http_con_read_response(con)
            self.assertEqual(data, b"")
            self.assertEqual(headers["connection"], "upgrade")
            self.assertEqual(headers["upgrade"], "edgedb-binary")
            self.assertEqual(status, 101)

            con = tb_protocol.new_sync_connection(
                con.sock, **self.get_connect_args()
            )
            con.sync_connect()
            con.sync_send(
                protocol.ExecuteScript(
                    headers=[],
                    script="SELECT 1",
                )
            )
            con.sync_recv_match(protocol.CommandComplete, status="SELECT")
            con.sync_recv_match(
                protocol.ReadyForCommand,
                transaction_state=protocol.TransactionState.NOT_IN_TRANSACTION,
            )

    def test_http_upgrade_success_mix_pipelining(self):
        with self.http_con(timeout=5) as con:
            conn_args = self.get_connect_args()
            con.sock.send(
                b"GET /server/status/ready HTTP/1.1\r\n"
                b"\r\n"
                b"GET /server/status/ready HTTP/1.1\r\n"
                b"Connection: upgrade\r\n"
                b"Upgrade: edgedb-binary\r\n"
                b"\r\n"
                + protocol.ClientHandshake(
                    major_ver=0,
                    minor_ver=10,
                    params=[
                        protocol.ConnectionParam(
                            name="user",
                            value=conn_args["user"],
                        ),
                        protocol.ConnectionParam(
                            name="database",
                            value=conn_args["database"],
                        ),
                    ],
                    extensions=[],
                ).dump()
            )

            resp = con.response_class(con.sock, method="GET")
            resp.begin()
            self.assertFalse(resp.will_close)
            self.assertEqual(resp.status, 200)
            self.assertIn(b"OK", resp.read())

            resp = con.response_class(con.sock, method="GET")
            resp.begin()
            self.assertFalse(resp.will_close)
            headers = {k.lower(): v.lower() for k, v in resp.getheaders()}
            self.assertEqual(headers["connection"], "upgrade")
            self.assertEqual(headers["upgrade"], "edgedb-binary")
            self.assertEqual(resp.status, 101)

            data = resp.fp.read1()  # data might've been buffered in the fp
            buffer = binwrapper.BinWrapper(io.BytesIO(data))
            mtype = buffer.read_ui8()
            data = buffer.read_bytes(buffer.read_i32() - 4)
            buffer = binwrapper.BinWrapper(io.BytesIO(data))

            msg_type = protocol.AuthenticationRequiredSASLMessage
            self.assertEqual(mtype, msg_type.mtype.default)
            kwargs = {}
            for fieldname, field in msg_type._fields.items():
                if fieldname in {"mtype", "message_length"}:
                    continue
                kwargs[fieldname] = field.parse(buffer)
            msg = msg_type(**kwargs)
            self.assertEqual(msg.auth_status, msg_type.auth_status.default)
            self.assertIn("SCRAM-SHA-256", msg.methods)

    def test_http_upgrade_wrong_protocol(self):
        with self.http_con() as con:
            con.request(
                "GET",
                "/server/status/ready",
                headers={"Connection": "upgrade", "Upgrade": "binary"},
            )
            data, headers, status = self.http_con_read_response(con)
            self.assertEqual(status, 200)
            self.assertIn(b"OK", data)

            con.request(
                "GET",
                "/server/status/ready",
            )
            data, headers, status = self.http_con_read_response(con)
            self.assertEqual(status, 200)
            self.assertIn(b"OK", data)

    def test_http_upgrade_wrong_protocol_with_pipelining(self):
        with self.http_con(timeout=5) as con:
            con.sock.send(
                b"GET /server/status/ready HTTP/1.1\r\n"
                b"Connection: upgrade\r\n"
                b"Upgrade: binary\r\n"
                b"\r\n"
                b"GET /server/status/ready HTTP/1.1\r\n"
                b"\r\n"
            )
            resp = con.response_class(con.sock, method="GET")
            resp.begin()
            self.assertFalse(resp.will_close)
            self.assertEqual(resp.status, 200)
            self.assertIn(b"OK", resp.read())

            resp = con.response_class(con.sock, method="GET")
            resp.begin()
            self.assertFalse(resp.will_close)
            self.assertEqual(resp.status, 200)
            self.assertIn(b"OK", resp.read())
