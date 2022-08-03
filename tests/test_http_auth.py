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


import base64
import struct

from edgedb import scram

from edb import protocol
from edb.testbase import http as tb
from edb.testbase import server as tb_server


class TestHttpAuth(tb.BaseHttpTest, tb_server.ConnectedTestCase):
    @classmethod
    def get_api_path(cls) -> str:
        return "/auth"

    def test_http_auth_scram(self):
        args = self.get_connect_args()

        with self.http_con() as con:
            _, headers, status = self.http_con_request(con, {}, path="token")
            self.assertEqual(status, 401)
            self.assertEqual(
                headers, headers | {"www-authenticate": "scram-sha-256"}
            )

        client_nonce = scram.generate_nonce()
        client_first, client_first_bare = scram.build_client_first_message(
            client_nonce, args["user"]
        )
        client_first_b64 = base64.b64encode(
            client_first.encode("ascii")
        ).decode("ascii")

        with self.http_con() as con:
            con.request(
                "GET",
                "/auth/token",
                headers={
                    "Authorization": f"SCRAM-SHA-256 data={client_first_b64}"
                },
            )
            resp = con.getresponse()
            headers = {k.lower(): v for k, v in resp.getheaders()}
            self.assertEqual(resp.status, 401)

        scheme, _, data = headers["www-authenticate"].partition(" ")
        self.assertEqual(scheme, "SCRAM-SHA-256")

        values = {}
        for kv_str in data.split():
            key, _, value = kv_str.rstrip(",").partition("=")
            values[key] = value

        self.assertIn("sid", values)
        self.assertIn("data", values)

        sid = values["sid"]
        server_first = base64.b64decode(values["data"])

        server_nonce, salt, itercount = scram.parse_server_first_message(
            server_first
        )

        client_final, expected_server_sig = scram.build_client_final_message(
            args["password"],
            salt,
            itercount,
            client_first_bare.encode("utf-8"),
            server_first,
            server_nonce,
        )
        client_final_b64 = base64.b64encode(
            client_final.encode("ascii")
        ).decode("ascii")

        with self.http_con() as con:
            con.request(
                "GET",
                "/auth/token",
                headers={
                    "Authorization": f"SCRAM-SHA-256 sid={sid} "
                    f"data={client_final_b64}"
                },
            )
            resp = con.getresponse()
            token = resp.read()
            headers = {k.lower(): v for k, v in resp.getheaders()}
            self.assertEqual(resp.status, 200)

        values = {}
        for kv_str in headers["authentication-info"].split():
            key, _, value = kv_str.rstrip(",").partition("=")
            values[key] = value

        self.assertEqual(values["sid"], sid)
        self.assertIn("data", values)
        server_final = base64.b64decode(values["data"])
        server_sig = scram.parse_server_final_message(server_final)
        self.assertEqual(server_sig, expected_server_sig)

        with self.http_con() as con:
            con.request(
                "POST",
                f"/db/{args['database']}",
                body=protocol.Execute(
                    annotations=[],
                    allowed_capabilities=protocol.Capability.ALL,
                    compilation_flags=protocol.CompilationFlag(0),
                    implicit_limit=0,
                    command_text="SELECT 42",
                    output_format=protocol.OutputFormat.JSON,
                    expected_cardinality=protocol.Cardinality.AT_MOST_ONE,
                    input_typedesc_id=b"\0" * 16,
                    output_typedesc_id=b"\0" * 16,
                    state_typedesc_id=b"\0" * 16,
                    arguments=b"",
                    state_data=b"",
                ).dump()
                + protocol.Sync().dump(),
                headers={
                    "Content-Type": "application/x.edgedb.v_1_0.binary",
                    "Authorization": f"Bearer {token.decode('ascii')}",
                    "X-EdgeDB-User": args["user"],
                },
            )
            content, headers, status = self.http_con_read_response(con)
        self.assertEqual(status, 200)
        self.assertEqual(
            headers,
            headers | {"content-type": "application/x.edgedb.v_1_0.binary"},
        )
        uint32_unpack = struct.Struct("!L").unpack
        msgs = []
        while content:
            mtype = content[0]
            (msize,) = uint32_unpack(content[1:5])
            msg = protocol.ServerMessage.parse(mtype, content[5 : msize + 1])
            msgs.append(msg)
            content = content[msize + 1 :]
        self.assertIsInstance(msgs[0], protocol.CommandDataDescription)
        self.assertIsInstance(msgs[1], protocol.Data)
        self.assertEqual(bytes(msgs[1].data[0].data), b"42")
        self.assertIsInstance(msgs[2], protocol.CommandComplete)
        self.assertEqual(msgs[2].status, "SELECT")
        self.assertIsInstance(msgs[3], protocol.ReadyForCommand)
        self.assertEqual(
            msgs[3].transaction_state,
            protocol.TransactionState.NOT_IN_TRANSACTION,
        )
