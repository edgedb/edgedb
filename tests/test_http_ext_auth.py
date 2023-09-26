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
import argon2

from typing import Any, Callable
from jwcrypto import jwt, jwk

from edb.testbase import http as tb


ph = argon2.PasswordHasher()

HTTP_TEST_PORT: contextvars.ContextVar[str] = contextvars.ContextVar(
    'HTTP_TEST_PORT'
)

GOOGLE_DISCOVERY_DOCUMENT = {
    "issuer": "https://accounts.google.com",
    "authorization_endpoint": ("https://accounts.google.com/o/oauth2/v2/auth"),
    "device_authorization_endpoint": (
        "https://oauth2.googleapis.com/device/code"
    ),
    "token_endpoint": ("https://oauth2.googleapis.com/token"),
    "userinfo_endpoint": ("https://openidconnect.googleapis.com/v1/userinfo"),
    "revocation_endpoint": ("https://oauth2.googleapis.com/revoke"),
    "jwks_uri": ("https://www.googleapis.com/oauth2/v3/certs"),
    "response_types_supported": [
        "code",
        "token",
        "id_token",
        "code token",
        "code id_token",
        "token id_token",
        "code token id_token",
        "none",
    ],
    "subject_types_supported": ["public"],
    "id_token_signing_alg_values_supported": ["RS256"],
    "scopes_supported": ["openid", "email", "profile"],
    "token_endpoint_auth_methods_supported": [
        "client_secret_post",
        "client_secret_basic",
    ],
    "claims_supported": [
        "aud",
        "email",
        "email_verified",
        "exp",
        "family_name",
        "given_name",
        "iat",
        "iss",
        "locale",
        "name",
        "picture",
        "sub",
    ],
    "code_challenge_methods_supported": ["plain", "S256"],
}

AZURE_DISCOVERY_DOCUMENT = {
    "token_endpoint": (
        "https://login.microsoftonline.com/common/oauth2/v2.0/token"
    ),
    "token_endpoint_auth_methods_supported": [
        "client_secret_post",
        "private_key_jwt",
        "client_secret_basic",
    ],
    "jwks_uri": "https://login.microsoftonline.com/common/discovery/v2.0/keys",
    "response_modes_supported": ["query", "fragment", "form_post"],
    "subject_types_supported": ["pairwise"],
    "id_token_signing_alg_values_supported": ["RS256"],
    "response_types_supported": [
        "code",
        "id_token",
        "code id_token",
        "id_token token",
    ],
    "scopes_supported": ["openid", "profile", "email", "offline_access"],
    "issuer": "https://login.microsoftonline.com/{tenantid}/v2.0",
    "request_uri_parameter_supported": False,
    "userinfo_endpoint": "https://graph.microsoft.com/oidc/userinfo",
    "authorization_endpoint": (
        "https://login.microsoftonline.com/common/oauth2/v2.0/authorize"
    ),
    "device_authorization_endpoint": (
        "https://login.microsoftonline.com/common/oauth2/v2.0/devicecode"
    ),
    "http_logout_supported": True,
    "frontchannel_logout_supported": True,
    "end_session_endpoint": (
        "https://login.microsoftonline.com/common/oauth2/v2.0/logout"
    ),
    "claims_supported": [
        "sub",
        "iss",
        "cloud_instance_name",
        "cloud_instance_host_name",
        "cloud_graph_host_name",
        "msgraph_host",
        "aud",
        "exp",
        "iat",
        "auth_time",
        "acr",
        "nonce",
        "preferred_username",
        "name",
        "tid",
        "ver",
        "at_hash",
        "c_hash",
        "email",
    ],
    "kerberos_endpoint": "https://login.microsoftonline.com/common/kerberos",
    "tenant_region_scope": None,
    "cloud_instance_name": "microsoftonline.com",
    "cloud_graph_host_name": "graph.windows.net",
    "msgraph_host": "graph.microsoft.com",
    "rbac_url": "https://pas.windows.net",
}

APPLE_DISCOVERY_DOCUMENT = {
    "issuer": "https://appleid.apple.com",
    "authorization_endpoint": "https://appleid.apple.com/auth/authorize",
    "token_endpoint": "https://appleid.apple.com/auth/token",
    "revocation_endpoint": "https://appleid.apple.com/auth/revoke",
    "jwks_uri": "https://appleid.apple.com/auth/keys",
    "response_types_supported": ["code"],
    "response_modes_supported": ["query", "fragment", "form_post"],
    "subject_types_supported": ["pairwise"],
    "id_token_signing_alg_values_supported": ["RS256"],
    "scopes_supported": ["openid", "email", "name"],
    "token_endpoint_auth_methods_supported": ["client_secret_post"],
    "claims_supported": [
        "aud",
        "email",
        "email_verified",
        "exp",
        "iat",
        "is_private_email",
        "iss",
        "nonce",
        "nonce_supported",
        "real_user_status",
        "sub",
        "transfer_sub",
    ],
}


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

    def log_message(self, *args):
        pass


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
        self._http_server.server_close()

    def __exit__(self, *exc):
        self._http_server.shutdown()
        self._http_runner.join()
        self._http_runner = None


SIGNING_KEY = 'a' * 32
GITHUB_SECRET = 'b' * 32
GOOGLE_SECRET = 'c' * 32
AZURE_SECRET = 'c' * 32
APPLE_SECRET = 'c' * 32


class TestHttpExtAuth(tb.ExtAuthTestCase):
    TRANSACTION_ISOLATION = False

    EXTENSION_SETUP = [
        f"""
        CONFIGURE CURRENT DATABASE SET
        ext::auth::AuthConfig::auth_signing_key := <str>'{SIGNING_KEY}';

        CONFIGURE CURRENT DATABASE SET
        ext::auth::AuthConfig::token_time_to_live := <duration>'24 hours';

        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::OAuthClientConfig {{
            provider_name := "github",
            url := "https://github.com",
            provider_id := <str>'{uuid.uuid4()}',
            secret := <str>'{GITHUB_SECRET}',
            client_id := <str>'{uuid.uuid4()}'
        }};

        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::OAuthClientConfig {{
            provider_name := "google",
            url := "https://accounts.google.com",
            provider_id := <str>'{uuid.uuid4()}',
            secret := <str>'{GOOGLE_SECRET}',
            client_id := <str>'{uuid.uuid4()}'
        }};

        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::OAuthClientConfig {{
            provider_name := "azure",
            url := "https://login.microsoftonline.com/common/v2.0",
            provider_id := <str>'{uuid.uuid4()}',
            secret := <str>'{AZURE_SECRET}',
            client_id := <str>'{uuid.uuid4()}'
        }};

        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::OAuthClientConfig {{
            provider_name := "apple",
            url := "https://appleid.apple.com",
            provider_id := <str>'{uuid.uuid4()}',
            secret := <str>'{APPLE_SECRET}',
            client_id := <str>'{uuid.uuid4()}'
        }};
        """,
        f"""
        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::PasswordClientConfig {{
            provider_name := "password",
            provider_id := <str>'{uuid.uuid4()}',
        }};
        """,
    ]

    @classmethod
    async def _wait_for_db_config(cls):
        dbname = cls.get_database_name()
        # Wait for the database config changes to propagate to the
        # server by watching a debug endpoint
        async for tr in cls.try_until_succeeds(ignore=AssertionError):
            async with tr:
                with cls.http_con() as http_con:
                    (
                        rdata,
                        _headers,
                        status,
                    ) = tb.ExtAuthTestCase.http_con_request(
                        http_con,
                        prefix="",
                        path="server-info",
                    )
                    data = json.loads(rdata)
                    config = data['databases'][dbname]['config']
                    if 'ext::auth::AuthConfig::providers' not in config:
                        raise AssertionError('database config not ready')

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.loop.run_until_complete(cls._wait_for_db_config())

    @classmethod
    def get_setup_script(cls):
        res = super().get_setup_script()

        import os.path

        # HACK: As a debugging cycle hack, when RELOAD is true, we reload the
        # extension package from the file, so we can test without a bootstrap.
        RELOAD = False

        if RELOAD:
            root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            with open(os.path.join(root, 'edb/lib/ext/auth.edgeql')) as f:
                contents = f.read()
            to_add = (
                '''
                drop extension package auth version '1.0';
                create extension auth;
            '''
                + contents
            )
            splice = '__internal_testmode := true;'
            res = res.replace(splice, splice + to_add)

        return res

    @classmethod
    def http_con_send_request(self, *args, headers=None, **kwargs):
        """Inject a test header.

        It's recognized by the server when explicitly run in the test mode.

        http_con_request() calls this method.
        """
        test_port = HTTP_TEST_PORT.get(None)
        if test_port is not None:
            if headers is None:
                headers = {}
            headers['x-edgedb-oauth-test-server'] = test_port
        return super().http_con_send_request(*args, headers=headers, **kwargs)

    async def get_oauth_client_config_by_provider(self, provider_name: str):
        return await self.con.query_single(
            """
            SELECT assert_exists(assert_single(
                cfg::Config.extensions[is ext::auth::AuthConfig]
                    .providers[is ext::auth::OAuthClientConfig]
                    { * } filter .provider_name = <str>$0
            ));
            """,
            provider_name,
        )

    async def get_password_client_config_by_provider(self, provider_name: str):
        return await self.con.query_single(
            """
            SELECT assert_exists(assert_single(
                cfg::Config.extensions[is ext::auth::AuthConfig]
                    .providers[is ext::auth::PasswordClientConfig]
                    { * } filter .provider_name = <str>$0
            ));
            """,
            provider_name,
        )

    async def get_auth_config_value(self, key: str):
        return await self.con.query_single(
            f"""
            SELECT assert_single(
                cfg::Config.extensions[is ext::auth::AuthConfig]
                    .{key}
            );
            """
        )

    async def get_signing_key(self):
        auth_signing_key = SIGNING_KEY
        key_bytes = base64.b64encode(auth_signing_key.encode())
        signing_key = jwk.JWK(k=key_bytes.decode(), kty="oct")
        return signing_key

    def generate_state_value(
        self,
        state_claims: dict[str, str | float],
        auth_signing_key: jwk.JWK,
    ) -> str:
        state_token = jwt.JWT(
            header={"alg": "HS256"},
            claims=state_claims,
        )
        state_token.make_signed_token(auth_signing_key)
        return state_token.serialize()

    async def extract_jwt_claims(self, raw_jwt: str):
        signing_key = await self.get_signing_key()
        jwt_token = jwt.JWT(key=signing_key, jwt=raw_jwt)
        claims = json.loads(jwt_token.claims)
        return claims

    def maybe_get_auth_token(self, headers: dict[str, str]) -> str | None:
        set_cookie = headers.get("set-cookie")
        if set_cookie is not None:
            (k, v) = set_cookie.split(";")[0].split("=")
            if k == "edgedb-session":
                return v

        return None

    async def extract_session_claims(self, headers: dict[str, str]):
        maybe_token = self.maybe_get_auth_token(headers)
        assert maybe_token is not None
        claims = await self.extract_jwt_claims(maybe_token)
        return claims

    async def test_http_auth_ext_github_authorize_01(self):
        with MockAuthProvider(), self.http_con() as http_con:
            provider_config = await self.get_oauth_client_config_by_provider(
                "github"
            )
            provider_id = provider_config.provider_id
            client_id = provider_config.client_id

            _, headers, status = self.http_con_request(
                http_con, {"provider": provider_id}, path="authorize"
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

            claims = await self.extract_jwt_claims(state[0])
            self.assertEqual(claims.get("provider"), provider_id)
            self.assertEqual(claims.get("iss"), self.http_addr)

            self.assertEqual(
                qs.get("redirect_uri"), [f"{self.http_addr}/callback"]
            )
            self.assertEqual(qs.get("client_id"), [client_id])

    async def test_http_auth_ext_github_callback_missing_provider_01(self):
        with MockAuthProvider(), self.http_con() as http_con:
            signing_key = await self.get_signing_key()

            expires_at = datetime.datetime.utcnow() + datetime.timedelta(
                minutes=5
            )
            missing_provider_state_claims = {
                "iss": self.http_addr,
                "exp": expires_at.astimezone().timestamp(),
            }
            state_token = self.generate_state_value(
                missing_provider_state_claims, signing_key
            )

            _, _, status = self.http_con_request(
                http_con,
                {"state": state_token, "code": "abc123"},
                path="callback",
            )

            self.assertEqual(status, 400)

    async def test_http_auth_ext_github_callback_wrong_key_01(self):
        with MockAuthProvider(), self.http_con() as http_con:
            provider_config = await self.get_oauth_client_config_by_provider(
                "github"
            )
            provider_id = provider_config.provider_id
            signing_key = jwk.JWK(
                k=base64.b64encode(("abcd" * 8).encode()).decode(), kty="oct"
            )

            expires_at = datetime.datetime.utcnow() + datetime.timedelta(
                minutes=5
            )
            missing_provider_state_claims = {
                "iss": self.http_addr,
                "provider": provider_id,
                "exp": expires_at.astimezone().timestamp(),
            }
            state_token_value = self.generate_state_value(
                missing_provider_state_claims, signing_key
            )

            _, _, status = self.http_con_request(
                http_con,
                {"state": state_token_value, "code": "abc123"},
                path="callback",
            )

            self.assertEqual(status, 400)

    async def test_http_auth_ext_github_unknown_provider_01(self):
        with MockAuthProvider(), self.http_con() as http_con:
            signing_key = await self.get_signing_key()

            expires_at = datetime.datetime.utcnow() + datetime.timedelta(
                minutes=5
            )
            state_claims = {
                "iss": self.http_addr,
                "provider": "beepboopbeep",
                "exp": expires_at.astimezone().timestamp(),
            }
            state_token = self.generate_state_value(state_claims, signing_key)

            _, _, status = self.http_con_request(
                http_con,
                {"state": state_token, "code": "abc123"},
                path="callback",
            )

            self.assertEqual(status, 400)

    async def test_http_auth_ext_github_callback_01(self):
        with MockAuthProvider() as mock_provider, self.http_con() as http_con:
            provider_config = await self.get_oauth_client_config_by_provider(
                "github"
            )
            provider_id = provider_config.provider_id
            client_id = provider_config.client_id
            client_secret = GITHUB_SECRET

            now = datetime.datetime.utcnow()
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
                        "updated_at": now.isoformat(),
                    },
                    200,
                )
            )

            signing_key = await self.get_signing_key()

            expires_at = now + datetime.timedelta(minutes=5)
            state_claims = {
                "iss": self.http_addr,
                "provider": str(provider_id),
                "exp": expires_at.astimezone().timestamp(),
                "redirect_to": f"{self.http_addr}/some/path",
            }
            state_token = self.generate_state_value(state_claims, signing_key)

            data, headers, status = self.http_con_request(
                http_con,
                {"state": state_token, "code": "abc123"},
                path="callback",
            )

            self.assertEqual(data, b"")
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

            identity = await self.con.query(
                """
                SELECT ext::auth::Identity
                FILTER .subject = '1'
                AND .issuer = 'https://github.com'
                """
            )
            self.assertEqual(len(identity), 1)

            session_claims = await self.extract_session_claims(headers)
            self.assertEqual(session_claims.get("sub"), str(identity[0].id))
            self.assertEqual(session_claims.get("iss"), str(self.http_addr))
            tomorrow = now + datetime.timedelta(hours=25)
            self.assertTrue(
                session_claims.get("exp") > now.astimezone().timestamp()
            )
            self.assertTrue(
                session_claims.get("exp") < tomorrow.astimezone().timestamp()
            )

            mock_provider.register_route_handler(*user_request)(
                (
                    {
                        "id": 1,
                        "login": "octocat",
                        "name": "monalisa octocat",
                        "email": "octocat+2@example.com",
                        "avatar_url": "http://example.com/example.jpg",
                        "updated_at": now.isoformat(),
                    },
                    200,
                )
            )
            (_, new_headers, _) = self.http_con_request(
                http_con,
                {"state": state_token, "code": "abc123"},
                path="callback",
            )

            same_identity = await self.con.query(
                """
                SELECT ext::auth::Identity
                FILTER .subject = '1'
                AND .issuer = 'https://github.com'
                """
            )
            self.assertEqual(len(same_identity), 1)
            self.assertEqual(identity[0].id, same_identity[0].id)

            new_session_claims = await self.extract_session_claims(new_headers)
            self.assertTrue(
                new_session_claims.get("exp") > session_claims.get("exp")
            )

    async def test_http_auth_ext_github_callback_failure_01(self):
        with MockAuthProvider() as mock_provider, self.http_con() as http_con:
            provider_config = await self.get_oauth_client_config_by_provider(
                "github"
            )
            provider_id = provider_config.provider_id

            now = datetime.datetime.utcnow()
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

            signing_key = await self.get_signing_key()

            expires_at = now + datetime.timedelta(minutes=5)
            state_claims = {
                "iss": self.http_addr,
                "provider": str(provider_id),
                "exp": expires_at.astimezone().timestamp(),
                "redirect_to": f"{self.http_addr}/some/path",
            }
            state_token = self.generate_state_value(state_claims, signing_key)

            data, headers, status = self.http_con_request(
                http_con,
                {
                    "state": state_token,
                    "error": "access_denied",
                    "error_description": (
                        "The user has denied your application access"
                    ),
                },
                path="callback",
            )

            self.assertEqual(data, b"")
            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            server_url = urllib.parse.urlparse(self.http_addr)
            url = urllib.parse.urlparse(location)
            self.assertEqual(url.scheme, server_url.scheme)
            self.assertEqual(url.hostname, server_url.hostname)
            self.assertEqual(url.path, f"{server_url.path}/some/path")
            self.assertEqual(
                url.query,
                "error=access_denied"
                "&error_description="
                "The+user+has+denied+your+application+access",
            )

    async def test_http_auth_ext_github_callback_failure_02(self):
        with MockAuthProvider() as mock_provider, self.http_con() as http_con:
            provider_config = await self.get_oauth_client_config_by_provider(
                "github"
            )
            provider_id = provider_config.provider_id

            now = datetime.datetime.utcnow()
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

            signing_key = await self.get_signing_key()

            expires_at = now + datetime.timedelta(minutes=5)
            state_claims = {
                "iss": self.http_addr,
                "provider": str(provider_id),
                "exp": expires_at.astimezone().timestamp(),
                "redirect_to": f"{self.http_addr}/some/path",
            }
            state_token = self.generate_state_value(state_claims, signing_key)

            data, headers, status = self.http_con_request(
                http_con,
                {
                    "state": state_token,
                    "error": "access_denied",
                },
                path="callback",
            )

            self.assertEqual(data, b"")
            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            server_url = urllib.parse.urlparse(self.http_addr)
            url = urllib.parse.urlparse(location)
            self.assertEqual(url.scheme, server_url.scheme)
            self.assertEqual(url.hostname, server_url.hostname)
            self.assertEqual(url.path, f"{server_url.path}/some/path")
            self.assertEqual(
                url.query,
                "error=access_denied",
            )

    async def test_http_auth_ext_google_callback_01(self):
        with MockAuthProvider() as mock_provider, self.http_con() as http_con:
            provider_config = await self.get_oauth_client_config_by_provider(
                "google"
            )
            provider_id = provider_config.provider_id
            client_id = provider_config.client_id
            client_secret = GOOGLE_SECRET

            now = datetime.datetime.utcnow()

            discovery_request = (
                "GET",
                "https://accounts.google.com",
                "/.well-known/openid-configuration",
            )
            mock_provider.register_route_handler(*discovery_request)(
                (
                    GOOGLE_DISCOVERY_DOCUMENT,
                    200,
                )
            )

            jwks_request = (
                "GET",
                "https://www.googleapis.com",
                "/oauth2/v3/certs",
            )
            # Generate a JWK Set
            k = jwk.JWK.generate(kty='RSA', size=4096)
            ks = jwk.JWKSet()
            ks.add(k)
            jwk_set: dict[str, Any] = ks.export(
                private_keys=False, as_dict=True
            )

            mock_provider.register_route_handler(*jwks_request)(
                (
                    jwk_set,
                    200,
                )
            )

            token_request = (
                "POST",
                "https://oauth2.googleapis.com",
                "/token",
            )
            id_token_claims = {
                "iss": "https://accounts.google.com",
                "sub": "1",
                "aud": client_id,
                "exp": (now + datetime.timedelta(minutes=5)).timestamp(),
                "iat": now.timestamp(),
                "email": "test@example.com",
            }
            id_token = jwt.JWT(header={"alg": "RS256"}, claims=id_token_claims)
            id_token.make_signed_token(k)

            mock_provider.register_route_handler(*token_request)(
                (
                    {
                        "access_token": "google_access_token",
                        "id_token": id_token.serialize(),
                        "scope": "openid",
                        "token_type": "bearer",
                    },
                    200,
                )
            )

            signing_key = await self.get_signing_key()

            expires_at = now + datetime.timedelta(minutes=5)
            state_claims = {
                "iss": self.http_addr,
                "provider": str(provider_id),
                "exp": expires_at.astimezone().timestamp(),
                "redirect_to": f"{self.http_addr}/some/path",
            }
            state_token = self.generate_state_value(state_claims, signing_key)

            data, headers, status = self.http_con_request(
                http_con,
                {"state": state_token, "code": "abc123"},
                path="callback",
            )

            print(f"data={data}")
            self.assertEqual(data, b"")
            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            server_url = urllib.parse.urlparse(self.http_addr)
            url = urllib.parse.urlparse(location)
            self.assertEqual(url.scheme, server_url.scheme)
            self.assertEqual(url.hostname, server_url.hostname)
            self.assertEqual(url.path, f"{server_url.path}/some/path")

            requests_for_discovery = mock_provider.requests[discovery_request]
            self.assertEqual(len(requests_for_discovery), 2)

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

            identity = await self.con.query(
                """
                SELECT ext::auth::Identity
                FILTER .subject = '1'
                AND .issuer = 'https://accounts.google.com'
                """
            )
            self.assertEqual(len(identity), 1)

            session_claims = await self.extract_session_claims(headers)
            self.assertEqual(session_claims.get("sub"), str(identity[0].id))
            self.assertEqual(session_claims.get("iss"), str(self.http_addr))
            tomorrow = now + datetime.timedelta(hours=25)
            self.assertTrue(
                session_claims.get("exp") > now.astimezone().timestamp()
            )
            self.assertTrue(
                session_claims.get("exp") < tomorrow.astimezone().timestamp()
            )

    async def test_http_auth_ext_google_authorize_01(self):
        with MockAuthProvider() as mock_provider, self.http_con() as http_con:
            provider_config = await self.get_oauth_client_config_by_provider(
                "google"
            )
            provider_id = provider_config.provider_id
            client_id = provider_config.client_id

            discovery_request = (
                "GET",
                "https://accounts.google.com",
                "/.well-known/openid-configuration",
            )
            mock_provider.register_route_handler(*discovery_request)(
                (
                    GOOGLE_DISCOVERY_DOCUMENT,
                    200,
                )
            )

            _, headers, status = self.http_con_request(
                http_con, {"provider": provider_id}, path="authorize"
            )

            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            url = urllib.parse.urlparse(location)
            qs = urllib.parse.parse_qs(url.query, keep_blank_values=True)
            self.assertEqual(url.scheme, "https")
            self.assertEqual(url.hostname, "accounts.google.com")
            self.assertEqual(url.path, "/o/oauth2/v2/auth")
            self.assertEqual(qs.get("scope"), ["openid profile email"])

            state = qs.get("state")
            assert state is not None

            claims = await self.extract_jwt_claims(state[0])
            self.assertEqual(claims.get("provider"), provider_id)
            self.assertEqual(claims.get("iss"), self.http_addr)

            self.assertEqual(
                qs.get("redirect_uri"), [f"{self.http_addr}/callback"]
            )
            self.assertEqual(qs.get("client_id"), [client_id])

            requests_for_discovery = mock_provider.requests[discovery_request]
            self.assertEqual(len(requests_for_discovery), 1)

    async def test_http_auth_ext_azure_authorize_01(self):
        with MockAuthProvider() as mock_provider, self.http_con() as http_con:
            provider_config = await self.get_oauth_client_config_by_provider(
                "azure"
            )
            provider_id = provider_config.provider_id
            client_id = provider_config.client_id

            discovery_request = (
                "GET",
                "https://login.microsoftonline.com/common/v2.0",
                "/.well-known/openid-configuration",
            )
            mock_provider.register_route_handler(*discovery_request)(
                (
                    AZURE_DISCOVERY_DOCUMENT,
                    200,
                )
            )

            _, headers, status = self.http_con_request(
                http_con, {"provider": provider_id}, path="authorize"
            )

            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            url = urllib.parse.urlparse(location)
            qs = urllib.parse.parse_qs(url.query, keep_blank_values=True)
            self.assertEqual(url.scheme, "https")
            self.assertEqual(url.hostname, "login.microsoftonline.com")
            self.assertEqual(url.path, "/common/oauth2/v2.0/authorize")
            self.assertEqual(qs.get("scope"), ["openid profile email"])

            state = qs.get("state")
            assert state is not None

            claims = await self.extract_jwt_claims(state[0])
            self.assertEqual(claims.get("provider"), provider_id)
            self.assertEqual(claims.get("iss"), self.http_addr)

            self.assertEqual(
                qs.get("redirect_uri"), [f"{self.http_addr}/callback"]
            )
            self.assertEqual(qs.get("client_id"), [client_id])

            requests_for_discovery = mock_provider.requests[discovery_request]
            self.assertEqual(len(requests_for_discovery), 1)

    async def test_http_auth_ext_azure_callback_01(self):
        with MockAuthProvider() as mock_provider, self.http_con() as http_con:
            provider_config = await self.get_oauth_client_config_by_provider(
                "azure"
            )
            provider_id = provider_config.provider_id
            client_id = provider_config.client_id
            client_secret = AZURE_SECRET

            now = datetime.datetime.utcnow()

            discovery_request = (
                "GET",
                "https://login.microsoftonline.com/common/v2.0",
                "/.well-known/openid-configuration",
            )
            mock_provider.register_route_handler(*discovery_request)(
                (
                    AZURE_DISCOVERY_DOCUMENT,
                    200,
                )
            )

            jwks_request = (
                "GET",
                "https://login.microsoftonline.com",
                "/common/discovery/v2.0/keys",
            )
            # Generate a JWK Set
            k = jwk.JWK.generate(kty='RSA', size=4096)
            ks = jwk.JWKSet()
            ks.add(k)
            jwk_set: dict[str, Any] = ks.export(
                private_keys=False, as_dict=True
            )

            mock_provider.register_route_handler(*jwks_request)(
                (
                    jwk_set,
                    200,
                )
            )

            token_request = (
                "POST",
                "https://login.microsoftonline.com",
                "/common/oauth2/v2.0/token",
            )
            id_token_claims = {
                "iss": "https://login.microsoftonline.com/common/v2.0",
                "sub": "1",
                "aud": client_id,
                "exp": (now + datetime.timedelta(minutes=5)).timestamp(),
                "iat": now.timestamp(),
                "email": "test@example.com",
            }
            id_token = jwt.JWT(header={"alg": "RS256"}, claims=id_token_claims)
            id_token.make_signed_token(k)

            mock_provider.register_route_handler(*token_request)(
                (
                    {
                        "access_token": "azure_access_token",
                        "id_token": id_token.serialize(),
                        "scope": "openid",
                        "token_type": "bearer",
                    },
                    200,
                )
            )

            signing_key = await self.get_signing_key()

            expires_at = now + datetime.timedelta(minutes=5)
            state_claims = {
                "iss": self.http_addr,
                "provider": str(provider_id),
                "exp": expires_at.astimezone().timestamp(),
                "redirect_to": f"{self.http_addr}/some/path",
            }
            state_token = self.generate_state_value(state_claims, signing_key)

            data, headers, status = self.http_con_request(
                http_con,
                {"state": state_token, "code": "abc123"},
                path="callback",
            )

            self.assertEqual(data, b"")
            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            server_url = urllib.parse.urlparse(self.http_addr)
            url = urllib.parse.urlparse(location)
            self.assertEqual(url.scheme, server_url.scheme)
            self.assertEqual(url.hostname, server_url.hostname)
            self.assertEqual(url.path, f"{server_url.path}/some/path")

            requests_for_discovery = mock_provider.requests[discovery_request]
            self.assertEqual(len(requests_for_discovery), 2)

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

    async def test_http_auth_ext_apple_authorize_01(self):
        with MockAuthProvider() as mock_provider, self.http_con() as http_con:
            provider_config = await self.get_oauth_client_config_by_provider(
                "apple"
            )
            provider_id = provider_config.provider_id
            client_id = provider_config.client_id

            discovery_request = (
                "GET",
                "https://appleid.apple.com",
                "/.well-known/openid-configuration",
            )
            mock_provider.register_route_handler(*discovery_request)(
                (
                    APPLE_DISCOVERY_DOCUMENT,
                    200,
                )
            )

            _, headers, status = self.http_con_request(
                http_con, {"provider": provider_id}, path="authorize"
            )

            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            url = urllib.parse.urlparse(location)
            qs = urllib.parse.parse_qs(url.query, keep_blank_values=True)
            self.assertEqual(url.scheme, "https")
            self.assertEqual(url.hostname, "appleid.apple.com")
            self.assertEqual(url.path, "/auth/authorize")
            self.assertEqual(qs.get("scope"), ["openid profile name"])

            state = qs.get("state")
            assert state is not None

            claims = await self.extract_jwt_claims(state[0])
            self.assertEqual(claims.get("provider"), provider_id)
            self.assertEqual(claims.get("iss"), self.http_addr)

            self.assertEqual(
                qs.get("redirect_uri"), [f"{self.http_addr}/callback"]
            )
            self.assertEqual(qs.get("client_id"), [client_id])

            requests_for_discovery = mock_provider.requests[discovery_request]
            self.assertEqual(len(requests_for_discovery), 1)

    async def test_http_auth_ext_apple_callback_01(self):
        with MockAuthProvider() as mock_provider, self.http_con() as http_con:
            provider_config = await self.get_oauth_client_config_by_provider(
                "apple"
            )
            provider_id = provider_config.provider_id
            client_id = provider_config.client_id
            client_secret = APPLE_SECRET

            now = datetime.datetime.utcnow()

            discovery_request = (
                "GET",
                "https://appleid.apple.com",
                "/.well-known/openid-configuration",
            )
            mock_provider.register_route_handler(*discovery_request)(
                (
                    APPLE_DISCOVERY_DOCUMENT,
                    200,
                )
            )

            jwks_request = (
                "GET",
                "https://appleid.apple.com",
                "/auth/keys",
            )
            # Generate a JWK Set
            k = jwk.JWK.generate(kty='RSA', size=4096)
            ks = jwk.JWKSet()
            ks.add(k)
            jwk_set: dict[str, Any] = ks.export(
                private_keys=False, as_dict=True
            )

            mock_provider.register_route_handler(*jwks_request)(
                (
                    jwk_set,
                    200,
                )
            )

            token_request = (
                "POST",
                "https://appleid.apple.com",
                "/auth/token",
            )
            id_token_claims = {
                "iss": "https://appleid.apple.com",
                "sub": "1",
                "aud": client_id,
                "exp": (now + datetime.timedelta(minutes=5)).timestamp(),
                "iat": now.timestamp(),
                "email": "test@example.com",
            }
            id_token = jwt.JWT(header={"alg": "RS256"}, claims=id_token_claims)
            id_token.make_signed_token(k)

            mock_provider.register_route_handler(*token_request)(
                (
                    {
                        "access_token": "apple_access_token",
                        "id_token": id_token.serialize(),
                        "scope": "openid",
                        "token_type": "bearer",
                    },
                    200,
                )
            )

            signing_key = await self.get_signing_key()

            expires_at = now + datetime.timedelta(minutes=5)
            state_claims = {
                "iss": self.http_addr,
                "provider": str(provider_id),
                "exp": expires_at.astimezone().timestamp(),
                "redirect_to": f"{self.http_addr}/some/path",
            }
            state_token = self.generate_state_value(state_claims, signing_key)

            data, headers, status = self.http_con_request(
                http_con,
                {"state": state_token, "code": "abc123"},
                path="callback",
            )

            self.assertEqual(data, b"")
            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            server_url = urllib.parse.urlparse(self.http_addr)
            url = urllib.parse.urlparse(location)
            self.assertEqual(url.scheme, server_url.scheme)
            self.assertEqual(url.hostname, server_url.hostname)
            self.assertEqual(url.path, f"{server_url.path}/some/path")

            requests_for_discovery = mock_provider.requests[discovery_request]
            self.assertEqual(len(requests_for_discovery), 2)

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

    async def test_http_auth_ext_local_password_register_form_01(self):
        with self.http_con() as http_con:
            provider_config = await self.get_password_client_config_by_provider(
                "password"
            )
            provider_id = provider_config.provider_id

            form_data = {
                "provider": provider_id,
                "email": "test@example.com",
                "password": "test_password",
                "redirect_to": "http://example.com/some/path",
            }
            form_data_encoded = urllib.parse.urlencode(form_data).encode()

            _, headers, status = self.http_con_request(
                http_con,
                None,
                path="register",
                method="POST",
                body=form_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            identity = await self.con.query(
                """
                SELECT ext::auth::LocalIdentity
                FILTER .<identity[is ext::auth::EmailPasswordFactor]
                       .email = 'test@example.com';
                """
            )

            self.assertEqual(len(identity), 1)

            self.assertEqual(status, 302)
            location = headers.get("location")
            assert location is not None
            auth_token = self.maybe_get_auth_token(headers)
            assert auth_token is not None
            parsed_location = urllib.parse.urlparse(location)
            parsed_query = urllib.parse.parse_qs(parsed_location.query)
            self.assertEqual(parsed_location.scheme, "http")
            self.assertEqual(parsed_location.netloc, "example.com")
            self.assertEqual(parsed_location.path, "/some/path")
            self.assertEqual(
                parsed_query,
                {
                    "identity_id": [str(identity[0].id)],
                    "auth_token": [auth_token],
                },
            )

            session_claims = await self.extract_session_claims(headers)
            self.assertEqual(session_claims.get("sub"), str(identity[0].id))
            self.assertEqual(session_claims.get("iss"), str(self.http_addr))
            now = datetime.datetime.utcnow()
            tomorrow = now + datetime.timedelta(hours=25)
            self.assertTrue(
                session_claims.get("exp") > now.astimezone().timestamp()
            )
            self.assertTrue(
                session_claims.get("exp") < tomorrow.astimezone().timestamp()
            )

            password_credential = await self.con.query(
                """
                SELECT ext::auth::EmailPasswordFactor { password_hash }
                FILTER .identity.id = <uuid>$identity
                """,
                identity=identity[0].id,
            )
            self.assertTrue(
                ph.verify(password_credential[0].password_hash, "test_password")
            )

            # Try to register the same user again (no redirect_to)
            _, _, conflict_status = self.http_con_request(
                http_con,
                None,
                path="register",
                method="POST",
                body=urllib.parse.urlencode(
                    {k: v for k, v in form_data.items() if k != 'redirect_to'}
                ).encode(),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(conflict_status, 409)

            # Try to register the same user again (no redirect_on_failure)
            _, redirect_to_headers, redirect_to_status = self.http_con_request(
                http_con,
                None,
                path="register",
                method="POST",
                body=form_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(redirect_to_status, 302)
            location = redirect_to_headers.get("location")
            assert location is not None
            parsed_location = urllib.parse.urlparse(location)
            parsed_query = urllib.parse.parse_qs(parsed_location.query)
            self.assertEqual(
                urllib.parse.urlunparse(
                    (
                        parsed_location.scheme,
                        parsed_location.netloc,
                        parsed_location.path,
                        '',
                        '',
                        '',
                    )
                ),
                form_data["redirect_to"],
            )

            self.assertEqual(
                parsed_query.get("error"),
                ["This user has already been registered"],
            )

            # Try to register the same user again (with redirect_on_failure)
            redirect_on_failure_url = "http://example.com/different/path"
            (
                _,
                redirect_on_failure_headers,
                redirect_on_failure_status,
            ) = self.http_con_request(
                http_con,
                None,
                path="register",
                method="POST",
                body=urllib.parse.urlencode(
                    {
                        **form_data,
                        "redirect_on_failure": redirect_on_failure_url,
                    }
                ).encode(),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(redirect_on_failure_status, 302)
            location = redirect_on_failure_headers.get("location")
            assert location is not None
            parsed_location = urllib.parse.urlparse(location)
            parsed_query = urllib.parse.parse_qs(parsed_location.query)
            self.assertEqual(
                urllib.parse.urlunparse(
                    (
                        parsed_location.scheme,
                        parsed_location.netloc,
                        parsed_location.path,
                        '',
                        '',
                        '',
                    )
                ),
                redirect_on_failure_url,
            )
            self.assertEqual(
                parsed_query.get("error"),
                ["This user has already been registered"],
            )

    async def test_http_auth_ext_local_password_register_json_02(self):
        with self.http_con() as http_con:
            provider_config = await self.get_password_client_config_by_provider(
                "password"
            )
            provider_id = provider_config.provider_id

            json_data = {
                "provider": provider_id,
                "email": "test2@example.com",
                "password": "test_password2",
            }
            json_data_encoded = json.dumps(json_data).encode()

            body, headers, status = self.http_con_request(
                http_con,
                None,
                path="register",
                method="POST",
                body=json_data_encoded,
                headers={"Content-Type": "application/json"},
            )

            self.assertEqual(status, 201)

            identity = await self.con.query(
                """
                SELECT ext::auth::LocalIdentity
                FILTER .<identity[is ext::auth::EmailPasswordFactor]
                       .email = 'test2@example.com';
                """
            )

            self.assertEqual(len(identity), 1)

            auth_token = self.maybe_get_auth_token(headers)
            assert auth_token is not None

            self.assertEqual(
                json.loads(body),
                {
                    "identity_id": str(identity[0].id),
                    "auth_token": auth_token,
                },
            )

            password_credential = await self.con.query(
                """
                SELECT ext::auth::EmailPasswordFactor { password_hash }
                FILTER .identity.id = <uuid>$identity
                """,
                identity=identity[0].id,
            )
            self.assertTrue(
                ph.verify(
                    password_credential[0].password_hash, "test_password2"
                )
            )

    async def test_http_auth_ext_local_password_register_form_missing_provider(
        self,
    ):
        with self.http_con() as http_con:
            form_data = {
                "email": "test@example.com",
                "password": "test_password",
            }
            form_data_encoded = urllib.parse.urlencode(form_data).encode()

            _, _, status = self.http_con_request(
                http_con,
                None,
                path="register",
                method="POST",
                body=form_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(status, 400)

    async def test_http_auth_ext_local_password_register_form_missing_password(
        self,
    ):
        with self.http_con() as http_con:
            provider_config = await self.get_password_client_config_by_provider(
                "password"
            )
            provider_id = provider_config.provider_id

            form_data = {
                "provider": provider_id,
                "email": "test@example.com",
            }
            form_data_encoded = urllib.parse.urlencode(form_data).encode()

            _, _, status = self.http_con_request(
                http_con,
                None,
                path="register",
                method="POST",
                body=form_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(status, 400)

    async def test_http_auth_ext_local_password_register_form_missing_email(
        self,
    ):
        with self.http_con() as http_con:
            provider_config = await self.get_password_client_config_by_provider(
                "password"
            )
            provider_id = provider_config.provider_id

            form_data = {
                "provider": provider_id,
                "password": "test_password",
            }
            form_data_encoded = urllib.parse.urlencode(form_data).encode()

            _, _, status = self.http_con_request(
                http_con,
                None,
                path="register",
                method="POST",
                body=form_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(status, 400)

    async def test_http_auth_ext_local_password_authenticate_01(self):
        with self.http_con() as http_con:
            provider_config = await self.get_password_client_config_by_provider(
                "password"
            )
            provider_id = provider_config.provider_id

            # Register a new user
            form_data = {
                "provider": provider_id,
                "email": "test_auth@example.com",
                "password": "test_auth_password",
            }
            form_data_encoded = urllib.parse.urlencode(form_data).encode()

            self.http_con_request(
                http_con,
                None,
                path="register",
                method="POST",
                body=form_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            auth_data = {
                "provider": form_data["provider"],
                "email": form_data["email"],
                "password": form_data["password"],
            }
            auth_data_encoded = urllib.parse.urlencode(auth_data).encode()

            body, headers, status = self.http_con_request(
                http_con,
                None,
                path="authenticate",
                method="POST",
                body=auth_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(status, 200)

            identity = await self.con.query(
                """
                SELECT ext::auth::LocalIdentity
                FILTER .<identity[is ext::auth::EmailPasswordFactor]
                       .email = 'test_auth@example.com';
                """
            )

            self.assertEqual(len(identity), 1)

            auth_token = self.maybe_get_auth_token(headers)
            assert auth_token is not None

            self.assertEqual(
                json.loads(body),
                {
                    "identity_id": str(identity[0].id),
                    "auth_token": auth_token,
                },
            )

            now = datetime.datetime.utcnow()
            tomorrow = now + datetime.timedelta(hours=25)
            session_claims = await self.extract_jwt_claims(auth_token)

            self.assertEqual(session_claims.get("sub"), str(identity[0].id))
            self.assertEqual(session_claims.get("iss"), str(self.http_addr))
            self.assertTrue(
                session_claims.get("exp") > now.astimezone().timestamp()
            )
            self.assertTrue(
                session_claims.get("exp") < tomorrow.astimezone().timestamp()
            )

            # Attempt to authenticate with wrong password
            auth_data_wrong_password = {
                "provider": form_data["provider"],
                "email": form_data["email"],
                "password": "wrong_password",
            }
            auth_data_encoded_wrong_password = urllib.parse.urlencode(
                auth_data_wrong_password
            ).encode()

            _, _, wrong_password_status = self.http_con_request(
                http_con,
                None,
                path="authenticate",
                method="POST",
                body=auth_data_encoded_wrong_password,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(wrong_password_status, 403)

            # Attempt to authenticate with a random email
            random_email = f"{str(uuid.uuid4())}@example.com"
            auth_data_random_handle = {
                "provider": form_data["provider"],
                "email": random_email,
                "password": form_data["password"],
            }
            auth_data_encoded_random_handle = urllib.parse.urlencode(
                auth_data_random_handle
            ).encode()

            _, _, wrong_handle_status = self.http_con_request(
                http_con,
                None,
                path="authenticate",
                method="POST",
                body=auth_data_encoded_random_handle,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(wrong_handle_status, 403)

            # Attempt to authenticate with a random email (redirect flow)
            auth_data_redirect_to = {
                "provider": form_data["provider"],
                "email": random_email,
                "password": form_data["password"],
                "redirect_to": "http://example.com/some/path",
            }
            auth_data_encoded_redirect_to = urllib.parse.urlencode(
                auth_data_redirect_to
            ).encode()

            _, redirect_to_headers, redirect_to_status = self.http_con_request(
                http_con,
                None,
                path="authenticate",
                method="POST",
                body=auth_data_encoded_redirect_to,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(redirect_to_status, 302)
            location = redirect_to_headers.get("location")
            assert location is not None
            parsed_location = urllib.parse.urlparse(location)
            parsed_query = urllib.parse.parse_qs(parsed_location.query)
            self.assertEqual(
                urllib.parse.urlunparse(
                    (
                        parsed_location.scheme,
                        parsed_location.netloc,
                        parsed_location.path,
                        '',
                        '',
                        '',
                    )
                ),
                auth_data_redirect_to["redirect_to"],
            )

            self.assertEqual(
                parsed_query.get("error"),
                [
                    (
                        "Could not find an Identity matching the provided "
                        "credentials"
                    )
                ],
            )

            # Attempt to authenticate with a random email
            # (redirect flow with redirect_on_failure)
            auth_data_redirect_on_failure = {
                "provider": form_data["provider"],
                "email": random_email,
                "password": form_data["password"],
                "redirect_to": "http://example.com/some/path",
                "redirect_on_failure": "http://example.com/failure/path",
            }
            auth_data_encoded_redirect_on_failure = urllib.parse.urlencode(
                auth_data_redirect_on_failure
            ).encode()

            (
                _,
                redirect_on_failure_headers,
                redirect_on_failure_status,
            ) = self.http_con_request(
                http_con,
                None,
                path="authenticate",
                method="POST",
                body=auth_data_encoded_redirect_on_failure,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(redirect_on_failure_status, 302)
            location = redirect_on_failure_headers.get("location")
            assert location is not None
            parsed_location = urllib.parse.urlparse(location)
            self.assertEqual(
                urllib.parse.urlunparse(
                    (
                        parsed_location.scheme,
                        parsed_location.netloc,
                        parsed_location.path,
                        '',
                        '',
                        '',
                    )
                ),
                auth_data_redirect_on_failure["redirect_on_failure"],
            )
