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

from typing import Any, Callable
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


ResponseType = tuple[dict[str, Any] | list[dict[str, Any]], int]


class MockAuthProvider:
    def __init__(self):
        self.has_started = threading.Event()
        self.routes: dict[
            tuple[str, str, str],
            ResponseType | Callable[[MockHttpServerHandler], ResponseType],
        ] = {}
        self.requests: dict[tuple[str, str, str], list[dict[str, Any]]] = {}

    def register_route_handler(
        self,
        method: str,
        server: str,
        path: str,
    ):
        def wrapper(
            handler: ResponseType
            | Callable[[MockHttpServerHandler], ResponseType]
        ):
            self.routes[(method, server, path)] = handler
            return handler

        return wrapper

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
        if key not in self.requests:
            self.requests[key] = []

        # Parse and save the request details
        parsed_path = urllib.parse.urlparse(path)
        request_details = {
            'headers': {k.lower(): v for k, v in dict(handler.headers).items()},
            'query_params': urllib.parse.parse_qs(parsed_path.query),
            'body': handler.rfile.read(
                int(handler.headers['Content-Length'])
            ).decode()
            if 'Content-Length' in handler.headers
            else None,
        }
        self.requests[key].append(request_details)

        if key not in self.routes:
            handler.send_error(404)
            return

        registered_handler = self.routes[key]

        if callable(registered_handler):
            try:
                response, status = registered_handler(handler)
            except Exception:
                handler.send_error(500)
                raise
        else:
            response, status = registered_handler
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
            client_id = await self.con.query_single(
                """SELECT assert_single(cfg::Config.xxx_github_client_id);"""
            )
            client_secret = await self.con.query_single(
                """
                SELECT assert_single(cfg::Config.xxx_github_client_secret);
                """
            )

            now = datetime.datetime.utcnow().isoformat()
            token_request = (
                "POST",
                "https://github.com",
                "/login/oauth/access_token",
            )
            mock_provider.register_route_handler(*token_request)(
                (
                    {
                        "access_token": "github_access_token",
                        "scope": "read:user",
                        "token_type": "bearer",
                    },
                    200,
                )
            )

            user_request = ("GET", "https://api.github.com", "/user")
            mock_provider.register_route_handler(*user_request)(
                (
                    {
                        "id": 1,
                        "login": "octocat",
                        "name": "monalisa octocat",
                        "email": "octocat@example.com",
                        "avatar_url": "http://example.com/example.jpg",
                        "updated_at": now,
                    },
                    200,
                )
            )

            emails_request = ("GET", "https://api.github.com", "/user/emails")
            mock_provider.register_route_handler(*emails_request)(
                (
                    [
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
                    200,
                )
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

            requests_for_token = mock_provider.requests[token_request]
            self.assertEqual(len(requests_for_token), 1)
            self.assertEqual(
                requests_for_token[0]["body"],
                json.dumps(
                    {
                        "grant_type": "authorization_code",
                        "code": "abc123",
                        "client_id": client_id,
                        "client_secret": client_secret,
                    }
                ),
            )

            requests_for_user = mock_provider.requests[user_request]
            self.assertEqual(len(requests_for_user), 1)
            self.assertEqual(
                requests_for_user[0]["headers"]["authorization"],
                "Bearer github_access_token",
            )

            requests_for_emails = mock_provider.requests[emails_request]
            self.assertEqual(len(requests_for_emails), 1)
            self.assertEqual(
                requests_for_emails[0]["headers"]["authorization"],
                "Bearer github_access_token",
            )
