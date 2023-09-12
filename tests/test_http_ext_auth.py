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


class TestHttpExtAuth(tb.ExtAuthTestCase):
    TRANSACTION_ISOLATION = False

    EXTENSION_SETUP = [
        f"""
        CONFIGURE CURRENT DATABASE SET
        ext::auth::AuthConfig::auth_signing_key := <str>'{'a' * 32}';
        """,
        """
        CONFIGURE CURRENT DATABASE SET
        ext::auth::AuthConfig::token_time_to_live := <duration>'24 hours';
        """,
        f"""
        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::ClientConfig {{
            provider_name := "github",
            url := "https://github.com",
            provider_id := <str>'{uuid.uuid4()}',
            secret := <str>'{"b" * 32}',
            client_id := <str>'{uuid.uuid4()}'
        }};
        """,
        f"""
        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::ClientConfig {{
            provider_name := "google",
            url := "https://accounts.google.com",
            provider_id := <str>'{uuid.uuid4()}',
            secret := <str>'{"c" * 32}',
            client_id := <str>'{uuid.uuid4()}'
        }};
        """,
        f"""
        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::ClientConfig {{
            provider_name := "azure",
            url := "https://login.microsoftonline.com/common/v2.0",
            provider_id := <str>'{uuid.uuid4()}',
            secret := <str>'{"c" * 32}',
            client_id := <str>'{uuid.uuid4()}'
        }};
        """,
        f"""
        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::ClientConfig {{
            provider_name := "apple",
            url := "https://appleid.apple.com",
            provider_id := <str>'{uuid.uuid4()}',
            secret := <str>'{"c" * 32}',
            client_id := <str>'{uuid.uuid4()}'
        }};
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

    async def get_client_config_by_provider(self, provider_name: str):
        return await self.con.query_single(
            """
            SELECT assert_exists(assert_single(
                cfg::Config.extensions[is ext::auth::AuthConfig]
                    .providers { * } filter .provider_name = <str>$0
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
        auth_signing_key = await self.get_auth_config_value("auth_signing_key")
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

    async def test_http_auth_ext_github_authorize_01(self):
        with MockAuthProvider(), self.http_con() as http_con:
            provider_config = await self.get_client_config_by_provider("github")
            provider_id = provider_config.provider_id
            client_id = provider_config.client_id

            signing_key = await self.get_signing_key()

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

            signed_token = jwt.JWT(
                key=signing_key, algs=["HS256"], jwt=state[0]
            )
            claims = json.loads(signed_token.claims)
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
            provider_config = await self.get_client_config_by_provider("github")
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
            provider_config = await self.get_client_config_by_provider("github")
            provider_id = provider_config.provider_id
            client_id = provider_config.client_id
            client_secret = provider_config.secret

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
                FILTER .sub = '1'
                AND .iss = 'https://github.com'
                AND .email = 'octocat@example.com'
                """
            )
            self.assertEqual(len(identity), 1)

            set_cookie = headers.get("set-cookie")
            assert set_cookie is not None
            (k, v) = set_cookie.split(";")[0].split("=")
            self.assertEqual(k, "edgedb-session")
            session_token = jwt.JWT(key=signing_key, jwt=v)
            session_claims = json.loads(session_token.claims)
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
            (_, headers, _) = self.http_con_request(
                http_con,
                {"state": state_token, "code": "abc123"},
                path="callback",
            )

            same_identity = await self.con.query(
                """
                SELECT ext::auth::Identity
                FILTER .sub = '1'
                AND .iss = 'https://github.com'
                AND .email = 'octocat+2@example.com'
                """
            )
            self.assertEqual(len(same_identity), 1)
            self.assertEqual(identity[0].id, same_identity[0].id)

            set_cookie = headers.get("set-cookie")
            assert set_cookie is not None
            (k, v) = set_cookie.split(";")[0].split("=")
            self.assertEqual(k, "edgedb-session")
            new_session_token = jwt.JWT(key=signing_key, jwt=v)
            new_session_claims = json.loads(new_session_token.claims)
            self.assertTrue(
                new_session_claims.get("exp") > session_claims.get("exp")
            )

    async def test_http_auth_ext_github_callback_failure_01(self):
        with MockAuthProvider() as mock_provider, self.http_con() as http_con:
            provider_config = await self.get_client_config_by_provider("github")
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
            provider_config = await self.get_client_config_by_provider("github")
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
            provider_config = await self.get_client_config_by_provider("google")
            provider_id = provider_config.provider_id
            client_id = provider_config.client_id
            client_secret = provider_config.secret

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
                FILTER .sub = '1'
                AND .iss = 'https://accounts.google.com'
                AND .email = 'test@example.com'
                """
            )
            self.assertEqual(len(identity), 1)

            set_cookie = headers.get("set-cookie")
            assert set_cookie is not None
            (k, v) = set_cookie.split(";")[0].split("=")
            self.assertEqual(k, "edgedb-session")
            session_token = jwt.JWT(key=signing_key, jwt=v)
            session_claims = json.loads(session_token.claims)
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
            provider_config = await self.get_client_config_by_provider("google")
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

            signing_key = await self.get_signing_key()

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

            signed_token = jwt.JWT(
                key=signing_key, algs=["HS256"], jwt=state[0]
            )
            claims = json.loads(signed_token.claims)
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
            provider_config = await self.get_client_config_by_provider("azure")
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

            signing_key = await self.get_signing_key()

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

            signed_token = jwt.JWT(
                key=signing_key, algs=["HS256"], jwt=state[0]
            )
            claims = json.loads(signed_token.claims)
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
            provider_config = await self.get_client_config_by_provider("azure")
            provider_id = provider_config.provider_id
            client_id = provider_config.client_id
            client_secret = provider_config.secret

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
            provider_config = await self.get_client_config_by_provider("apple")
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

            signing_key = await self.get_signing_key()

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

            signed_token = jwt.JWT(
                key=signing_key, algs=["HS256"], jwt=state[0]
            )
            claims = json.loads(signed_token.claims)
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
            provider_config = await self.get_client_config_by_provider("apple")
            provider_id = provider_config.provider_id
            client_id = provider_config.client_id
            client_secret = provider_config.secret

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
