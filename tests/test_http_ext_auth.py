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


import contextvars
import urllib.parse
import uuid
import json
import base64
import datetime
import http.server
import threading

from typing import Any
from jwcrypto import jwt, jwk

from edb.testbase import http as tb


HTTP_TEST_PORT: contextvars.ContextVar[str] = contextvars.ContextVar(
    'HTTP_TEST_PORT'
)


class MockHttpServerHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.close_connection = False
        server, path = self.path.lstrip('/').split('/', 1)
        server = urllib.parse.unquote(server)
        self.server.owner.handle_request('GET', server, path, self)

    def do_POST(self):
        self.close_connection = False
        server, path = self.path.lstrip('/').split('/', 1)
        server = urllib.parse.unquote(server)
        self.server.owner.handle_request('POST', server, path, self)


class MockAuthProvider:
    def __init__(self):
        self.has_started = threading.Event()
        self.routes: dict[
            tuple[str, str, str],
            tuple[dict[str, Any] | list[dict[str, Any]], int],
        ] = {}

    def register_route(
        self,
        method: str,
        server: str,
        path: str,
        response: dict[str, Any] | list[dict[str, Any]],
        status: int = 200,
    ):
        self.routes[(method, server, path)] = (response, status)

    def handle_request(
        self,
        method: str,
        server: str,
        path: str,
        handler: MockHttpServerHandler,
    ):
        # `handler` is documented here:
        # https://docs.python.org/3/library/http.server.html#http.server.BaseHTTPRequestHandler
        key = (method, server, path)
        if key not in self.routes:
            handler.send_error(404)
            return

        response, status = self.routes[key]
        data = json.dumps(response).encode()

        handler.send_response(status)
        handler.send_header('Content-Type', 'application/json')
        handler.send_header('Content-Length', str(len(data)))
        handler.end_headers()
        handler.wfile.write(data)

    def __enter__(self):
        assert not hasattr(self, '_http_runner')
        self._http_runner = threading.Thread(target=self._http_worker)
        self._http_runner.start()
        self.has_started.wait()
        HTTP_TEST_PORT.set(f'http://{self._address[0]}:{self._address[1]}/')
        return self

    def _http_worker(self):
        self._http_server = http.server.HTTPServer(
            ('localhost', 0), MockHttpServerHandler
        )
        self._http_server.owner = self
        self._address = self._http_server.server_address
        self.has_started.set()
        self._http_server.serve_forever(poll_interval=0.01)

    def __exit__(self, *exc):
        self._http_server.shutdown()
        self._http_runner.join()
        self._http_runner = None


class TestHttpExtAuth(tb.ExtAuthTestCase):
    TRANSACTION_ISOLATION = False

    SETUP = [
        f"""
        CONFIGURE CURRENT DATABASE
        SET xxx_auth_signing_key := <str>'{"a" * 32}';
        """,
        f"""
        CONFIGURE CURRENT DATABASE
        SET xxx_github_client_secret := <str>'{"b" * 32}';
        """,
        f"""
        CONFIGURE CURRENT DATABASE
        SET xxx_github_client_id := <str>'{uuid.uuid4()}';
        """,
    ]

    def http_con_send_request(self, *args, headers=None, **kwargs):
        """Inject a test header.

        It's recognized by the server when explicitly run in the test mode.

        http_con_request() calls this method.
        """
        test_port = HTTP_TEST_PORT.get()
        if test_port:
            if headers is None:
                headers = {}
            headers['x-edgedb-oauth-test-server'] = test_port
        return super().http_con_send_request(*args, headers=headers, **kwargs)

    async def test_http_auth_ext_github_authorize_01(self):
        with MockAuthProvider(), self.http_con() as http_con:
            client_id = await self.con.query_single(
                """SELECT assert_single(cfg::Config.xxx_github_client_id);"""
            )

            auth_signing_key = await self.con.query_single(
                """SELECT assert_single(cfg::Config.xxx_auth_signing_key);"""
            )

            _, headers, status = self.http_con_request(
                http_con, {"provider": "github"}, path="authorize"
            )

            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            url = urllib.parse.urlparse(location)
            qs = urllib.parse.parse_qs(url.query, keep_blank_values=True)
            self.assertEqual(url.scheme, "https")
            self.assertEqual(url.hostname, "github.com")
            self.assertEqual(url.path, "/login/oauth/authorize")
            self.assertEqual(qs.get("scope"), ["read:user user:email"])

            state = qs.get("state")
            assert state is not None

            key_bytes = base64.b64encode(auth_signing_key.encode())
            key = jwk.JWK(k=key_bytes.decode(), kty="oct")
            signed_token = jwt.JWT(key=key, algs=["HS256"], jwt=state[0])
            claims = json.loads(signed_token.claims)
            self.assertEqual(claims.get("provider"), "github")
            self.assertEqual(claims.get("iss"), self.http_addr)

            self.assertEqual(
                qs.get("redirect_uri"), [f"{self.http_addr}/callback"]
            )
            self.assertEqual(qs.get("client_id"), [client_id])

    async def test_http_auth_ext_github_callback_missing_provider_01(self):
        with MockAuthProvider(), self.http_con() as http_con:
            auth_signing_key = await self.con.query_single(
                """SELECT assert_single(cfg::Config.xxx_auth_signing_key);"""
            )

            expires_at = datetime.datetime.utcnow() + datetime.timedelta(
                minutes=5
            )
            missing_provider_state_claims = {
                "iss": self.http_addr,
                "exp": expires_at.astimezone().timestamp(),
            }
            state_token = jwt.JWT(
                header={"alg": "HS256"},
                claims=missing_provider_state_claims,
            )

            key_bytes = base64.b64encode(auth_signing_key.encode())
            key = jwk.JWK(k=key_bytes.decode(), kty="oct")
            state_token.make_signed_token(key)

            _, _, status = self.http_con_request(
                http_con,
                {"state": state_token.serialize(), "code": "abc123"},
                path="callback",
            )

            self.assertEqual(status, 400)

    async def test_http_auth_ext_github_callback_wrong_key_01(self):
        with MockAuthProvider(), self.http_con() as http_con:
            auth_signing_key = "abcd" * 8

            expires_at = datetime.datetime.utcnow() + datetime.timedelta(
                minutes=5
            )
            missing_provider_state_claims = {
                "iss": self.http_addr,
                "provider": "github",
                "exp": expires_at.astimezone().timestamp(),
            }
            state_token = jwt.JWT(
                header={"alg": "HS256"},
                claims=missing_provider_state_claims,
            )

            key_bytes = base64.b64encode(auth_signing_key.encode())
            key = jwk.JWK(k=key_bytes.decode(), kty="oct")
            state_token.make_signed_token(key)

            _, _, status = self.http_con_request(
                http_con,
                {"state": state_token.serialize(), "code": "abc123"},
                path="callback",
            )

            self.assertEqual(status, 400)

    async def test_http_auth_ext_github_unknown_provider_01(self):
        with MockAuthProvider(), self.http_con() as http_con:
            auth_signing_key = await self.con.query_single(
                """SELECT assert_single(cfg::Config.xxx_auth_signing_key);"""
            )

            expires_at = datetime.datetime.utcnow() + datetime.timedelta(
                minutes=5
            )
            state_claims = {
                "iss": self.http_addr,
                "provider": "beepboopbeep",
                "exp": expires_at.astimezone().timestamp(),
            }
            state_token = jwt.JWT(
                header={"alg": "HS256"},
                claims=state_claims,
            )

            key_bytes = base64.b64encode(auth_signing_key.encode())
            key = jwk.JWK(k=key_bytes.decode(), kty="oct")
            state_token.make_signed_token(key)

            _, _, status = self.http_con_request(
                http_con,
                {"state": state_token.serialize(), "code": "abc123"},
                path="callback",
            )

            self.assertEqual(status, 400)

    async def test_http_auth_ext_github_callback_01(self):
        with MockAuthProvider() as mock_provider, self.http_con() as http_con:
            now = datetime.datetime.utcnow().isoformat()
            mock_provider.register_route(
                method="POST",
                server="https://github.com",
                path="/login/oauth/access_token",
                response={
                    "access_token": "abc123",
                    "scope": "read:user",
                    "token_type": "bearer",
                },
            )

            mock_provider.register_route(
                method="GET",
                server="https://api.github.com",
                path="/user",
                response={
                    "id": 1,
                    "login": "octocat",
                    "name": "monalisa octocat",
                    "email": "octocat@example.com",
                    "avatar_url": "http://example.com/example.jpg",
                    "updated_at": now,
                },
            )

            mock_provider.register_route(
                method="GET",
                server="https://api.github.com",
                path="/user/emails",
                response=[
                    {
                        "email": "octocat@example.com",
                        "verified": True,
                        "primary": True,
                    },
                    {
                        "email": "octocat+2@example.com",
                        "verified": False,
                        "primary": False,
                    },
                ],
            )

            auth_signing_key = await self.con.query_single(
                """SELECT assert_single(cfg::Config.xxx_auth_signing_key);"""
            )

            expires_at = datetime.datetime.utcnow() + datetime.timedelta(
                minutes=5
            )
            state_claims = {
                "iss": self.http_addr,
                "provider": "github",
                "exp": expires_at.astimezone().timestamp(),
                "redirect_to": f"{self.http_addr}/some/path",
            }
            state_token = jwt.JWT(
                header={"alg": "HS256"},
                claims=state_claims,
            )

            key_bytes = base64.b64encode(auth_signing_key.encode())
            key = jwk.JWK(k=key_bytes.decode(), kty="oct")
            state_token.make_signed_token(key)

            _data, headers, status = self.http_con_request(
                http_con,
                {"state": state_token.serialize(), "code": "abc123"},
                path="callback",
            )

            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            server_url = urllib.parse.urlparse(self.http_addr)
            url = urllib.parse.urlparse(location)
            self.assertEqual(url.scheme, server_url.scheme)
            self.assertEqual(url.hostname, server_url.hostname)
            self.assertEqual(url.path, f"{server_url.path}/some/path")
