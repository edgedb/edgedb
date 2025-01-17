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
import argon2
import os
import pickle
import re
import hashlib
import hmac

from typing import Any, Optional, cast
from jwcrypto import jwt, jwk
from email.message import EmailMessage

from edgedb import QueryAssertionError
from edb.testbase import http as tb
from edb.common import assert_data_shape
from edb.server.protocol.auth_ext import util as auth_util

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

SLACK_DISCOVERY_DOCUMENT = {
    "issuer": "https://slack.com",
    "authorization_endpoint": "https://slack.com/openid/connect/authorize",
    "token_endpoint": "https://slack.com/api/openid.connect.token",
    "userinfo_endpoint": "https://slack.com/api/openid.connect.userInfo",
    "jwks_uri": "https://slack.com/openid/connect/keys",
    "scopes_supported": ["openid", "profile", "email"],
    "response_types_supported": ["code"],
    "response_modes_supported": ["query"],
    "grant_types_supported": ["authorization_code"],
    "subject_types_supported": ["public"],
    "id_token_signing_alg_values_supported": ["RS256"],
    "claims_supported": ["sub", "auth_time", "iss"],
    "claims_parameter_supported": False,
    "request_parameter_supported": False,
    "request_uri_parameter_supported": True,
    "token_endpoint_auth_methods_supported": [
        "client_secret_post",
        "client_secret_basic",
    ],
}

GENERIC_OIDC_DISCOVERY_DOCUMENT = {
    "issuer": "https://example.com",
    "authorization_endpoint": "https://example.com/auth",
    "token_endpoint": "https://example.com/token",
    "userinfo_endpoint": "https://example.com/userinfo",
    "jwks_uri": "https://example.com/jwks",
    "scopes_supported": ["openid", "profile", "email"],
    "response_types_supported": ["code"],
    "response_modes_supported": ["query"],
    "grant_types_supported": ["authorization_code"],
    "subject_types_supported": ["public"],
    "id_token_signing_alg_values_supported": ["RS256"],
    "claims_supported": ["sub", "auth_time", "iss"],
    "claims_parameter_supported": False,
    "request_parameter_supported": False,
    "request_uri_parameter_supported": True,
    "token_endpoint_auth_methods_supported": [
        "client_secret_post",
        "client_secret_basic",
    ],
}


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)


SIGNING_KEY = 'a' * 32
GITHUB_SECRET = 'b' * 32
GOOGLE_SECRET = 'c' * 32
AZURE_SECRET = 'c' * 32
APPLE_SECRET = 'c' * 32
DISCORD_SECRET = 'd' * 32
SLACK_SECRET = 'd' * 32
GENERIC_OIDC_SECRET = 'e' * 32
APP_NAME = "Test App" * 13
LOGO_URL = "http://example.com/logo.png"
DARK_LOGO_URL = "http://example.com/darklogo.png"
BRAND_COLOR = "f0f8ff"
SENDER = f"sender@example.com"


class TestHttpExtAuth(tb.ExtAuthTestCase):
    TRANSACTION_ISOLATION = False
    PARALLELISM_GRANULARITY = 'suite'

    SETUP = [
        f"""
        CONFIGURE CURRENT DATABASE INSERT cfg::SMTPProviderConfig {{
            name := "email_hosting_is_easy",
            sender := "{SENDER}",
        }};

        CONFIGURE CURRENT DATABASE SET
        current_email_provider_name := "email_hosting_is_easy";

        CONFIGURE CURRENT DATABASE SET
        ext::auth::AuthConfig::auth_signing_key := '{SIGNING_KEY}';

        CONFIGURE CURRENT DATABASE SET
        ext::auth::AuthConfig::token_time_to_live := <duration>'24 hours';

        CONFIGURE CURRENT DATABASE SET
        ext::auth::AuthConfig::app_name := '{APP_NAME}';

        CONFIGURE CURRENT DATABASE SET
        ext::auth::AuthConfig::logo_url := '{LOGO_URL}';

        CONFIGURE CURRENT DATABASE SET
        ext::auth::AuthConfig::dark_logo_url := '{DARK_LOGO_URL}';

        CONFIGURE CURRENT DATABASE SET
        ext::auth::AuthConfig::brand_color := '{BRAND_COLOR}';

        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::UIConfig {{
          redirect_to := 'https://example.com/app',
          redirect_to_on_signup := 'https://example.com/signup/app',
        }};

        CONFIGURE CURRENT DATABASE SET
        ext::auth::AuthConfig::allowed_redirect_urls := {{
            'https://example.com/app'
        }};

        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::GitHubOAuthProvider {{
            secret := '{GITHUB_SECRET}',
            client_id := '{uuid.uuid4()}',
        }};

        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::GoogleOAuthProvider {{
            secret := '{GOOGLE_SECRET}',
            client_id := '{uuid.uuid4()}',
        }};

        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::AzureOAuthProvider {{
            secret := '{AZURE_SECRET}',
            client_id := '{uuid.uuid4()}',
            additional_scope := 'offline_access',
        }};

        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::AppleOAuthProvider {{
            secret := '{APPLE_SECRET}',
            client_id := '{uuid.uuid4()}',
        }};

        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::DiscordOAuthProvider {{
            secret := '{DISCORD_SECRET}',
            client_id := '{uuid.uuid4()}',
        }};

        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::SlackOAuthProvider {{
            secret := '{SLACK_SECRET}',
            client_id := '{uuid.uuid4()}',
        }};

        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::OpenIDConnectProvider {{
            secret := '{GENERIC_OIDC_SECRET}',
            client_id := '{uuid.uuid4()}',
            name := 'generic_oidc',
            display_name := 'My Generic OIDC Provider',
            issuer_url := 'https://example.com',
            additional_scope := 'custom_provider_scope_string',
        }};

        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::EmailPasswordProviderConfig {{
            require_verification := false,
        }};

        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::WebAuthnProviderConfig {{
            relying_party_origin := 'https://example.com:8080',
            require_verification := false,
        }};

        CONFIGURE CURRENT DATABASE
        INSERT ext::auth::MagicLinkProviderConfig {{}};
        """,
    ]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.loop.run_until_complete(
            cls._wait_for_db_config('ext::auth::AuthConfig::providers')
        )

    mock_oauth_server: tb.MockHttpServer
    mock_net_server: tb.MockHttpServer

    def setUp(self):
        self.mock_oauth_server = tb.MockHttpServer(
            handler_type=tb.MultiHostMockHttpServerHandler
        )
        self.mock_oauth_server.start()
        HTTP_TEST_PORT.set(self.mock_oauth_server.get_base_url())

        self.mock_net_server = tb.MockHttpServer()
        self.mock_net_server.start()
        super().setUp()

    def tearDown(self):
        if self.mock_oauth_server is not None:
            self.mock_oauth_server.stop()
        if self.mock_net_server is not None:
            self.mock_net_server.stop()
        self.mock_oauth_server = None
        super().tearDown()

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

    async def get_provider_config_by_name(self, fqn: str):
        return await self.con.query_single(
            """
            SELECT assert_exists(
                cfg::Config.extensions[is ext::auth::AuthConfig].providers {
                    *,
                    [is ext::auth::OAuthProviderConfig].client_id,
                    [is ext::auth::OAuthProviderConfig].additional_scope,
                } filter .name = <str>$0
            );
            """,
            fqn,
        )

    async def get_builtin_provider_config_by_name(self, provider_name: str):
        return await self.get_provider_config_by_name(
            f"builtin::{provider_name}"
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

    async def extract_jwt_claims(self, raw_jwt: str, info: str | None = None):
        input_key_material = await self.get_signing_key()
        if info is not None:
            signing_key = auth_util.derive_key(input_key_material, info)
        else:
            signing_key = input_key_material

        jwt_token = jwt.JWT(key=signing_key, jwt=raw_jwt)
        claims = json.loads(jwt_token.claims)
        return claims

    def maybe_get_cookie_value(
        self, headers: dict[str, str], name: str
    ) -> Optional[str]:
        set_cookie = headers.get("set-cookie")
        if set_cookie is not None:
            (k, v) = set_cookie.split(";", 1)[0].split("=", 1)
            if k == name:
                return v

        return None

    def maybe_get_auth_token(self, headers: dict[str, str]) -> Optional[str]:
        return self.maybe_get_cookie_value(headers, "edgedb-session")

    async def test_http_auth_ext_github_authorize_01(self):
        with self.http_con() as http_con:
            provider_config = await self.get_builtin_provider_config_by_name(
                "oauth_github"
            )
            provider_name = provider_config.name
            client_id = provider_config.client_id
            redirect_to = f"{self.http_addr}/some/path"
            callback_url = f"{self.http_addr}/some/callback/url"
            challenge = (
                base64.urlsafe_b64encode(
                    hashlib.sha256(
                        base64.urlsafe_b64encode(os.urandom(43)).rstrip(b'=')
                    ).digest()
                )
                .rstrip(b'=')
                .decode()
            )
            query = {
                "provider": provider_name,
                "redirect_to": redirect_to,
                "challenge": challenge,
                "callback_url": callback_url,
            }

            _, headers, status = self.http_con_request(
                http_con,
                query,
                path="authorize",
            )

            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            url = urllib.parse.urlparse(location)
            qs = urllib.parse.parse_qs(url.query, keep_blank_values=True)
            self.assertEqual(url.scheme, "https")
            self.assertEqual(url.hostname, "github.com")
            self.assertEqual(url.path, "/login/oauth/authorize")
            self.assertEqual(qs.get("scope"), ["read:user user:email "])

            state = qs.get("state")
            assert state is not None

            claims = await self.extract_jwt_claims(state[0])
            self.assertEqual(claims.get("provider"), provider_name)
            self.assertEqual(claims.get("iss"), self.http_addr)
            self.assertEqual(claims.get("redirect_to"), redirect_to)

            self.assertEqual(
                qs.get("redirect_uri"), [callback_url]
            )
            self.assertEqual(qs.get("client_id"), [client_id])

            pkce = await self.con.query(
                """
                select ext::auth::PKCEChallenge
                filter .challenge = <str>$challenge
                """,
                challenge=challenge,
            )
            self.assertEqual(len(pkce), 1)

            _, _, repeat_status = self.http_con_request(
                http_con,
                query,
                path="authorize",
            )
            self.assertEqual(repeat_status, 302)

            repeat_pkce = await self.con.query_single(
                """
                select ext::auth::PKCEChallenge
                filter .challenge = <str>$challenge
                """,
                challenge=challenge,
            )
            self.assertEqual(pkce[0].id, repeat_pkce.id)

    async def test_http_auth_ext_github_callback_missing_provider_01(self):
        with self.http_con() as http_con:
            signing_key = await self.get_signing_key()

            expires_at = utcnow() + datetime.timedelta(minutes=5)
            missing_provider_state_claims = {
                "iss": self.http_addr,
                "exp": expires_at.timestamp(),
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
        with self.http_con() as http_con:
            provider_config = await self.get_builtin_provider_config_by_name(
                "oauth_github"
            )
            provider_name = provider_config.name
            signing_key = jwk.JWK(
                k=base64.b64encode(("abcd" * 8).encode()).decode(), kty="oct"
            )

            expires_at = utcnow() + datetime.timedelta(minutes=5)
            missing_provider_state_claims = {
                "iss": self.http_addr,
                "provider": provider_name,
                "exp": expires_at.timestamp(),
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
        with self.http_con() as http_con:
            signing_key = await self.get_signing_key()

            expires_at = utcnow() + datetime.timedelta(minutes=5)
            state_claims = {
                "iss": self.http_addr,
                "provider": "beepboopbeep",
                "exp": expires_at.timestamp(),
            }
            state_token = self.generate_state_value(state_claims, signing_key)

            body, _, status = self.http_con_request(
                http_con,
                {"state": state_token, "code": "abc123"},
                path="callback",
            )

            try:
                body_json = json.loads(body)
                self.assertIsNotNone(body_json)
            except json.JSONDecodeError:
                self.fail("Failed to parse JSON from response body")

            self.assertEqual(status, 400)
            self.assertEqual(
                body_json.get("error"),
                {
                    "type": "InvalidData",
                    "message": "Invalid state token",
                },
            )

    async def test_http_auth_ext_github_callback_01(self):
        with self.http_con() as http_con:
            provider_config = await self.get_builtin_provider_config_by_name(
                "oauth_github"
            )
            provider_name = provider_config.name
            client_id = provider_config.client_id
            client_secret = GITHUB_SECRET

            now = utcnow()
            token_request = (
                "POST",
                "https://github.com",
                "login/oauth/access_token",
            )
            self.mock_oauth_server.register_route_handler(*token_request)(
                (
                    json.dumps(
                        {
                            "access_token": "github_access_token",
                            "scope": "read:user",
                            "token_type": "bearer",
                        }
                    ),
                    200,
                )
            )

            user_request = ("GET", "https://api.github.com", "user")
            self.mock_oauth_server.register_route_handler(*user_request)(
                (
                    json.dumps(
                        {
                            "id": 1,
                            "login": "octocat",
                            "name": "monalisa octocat",
                            "email": "octocat@example.com",
                            "avatar_url": "https://example.com/example.jpg",
                            "updated_at": now.isoformat(),
                        }
                    ),
                    200,
                )
            )

            challenge = (
                base64.urlsafe_b64encode(
                    hashlib.sha256(
                        base64.urlsafe_b64encode(os.urandom(43)).rstrip(b'=')
                    ).digest()
                )
                .rstrip(b'=')
                .decode()
            )
            await self.con.query(
                """
                insert ext::auth::PKCEChallenge {
                    challenge := <str>$challenge,
                }
                """,
                challenge=challenge,
            )

            signing_key = await self.get_signing_key()

            expires_at = now + datetime.timedelta(minutes=5)
            state_claims = {
                "iss": self.http_addr,
                "provider": str(provider_name),
                "exp": expires_at.timestamp(),
                "redirect_to": f"{self.http_addr}/some/path",
                "challenge": challenge,
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

            requests_for_token = self.mock_oauth_server.requests[token_request]
            self.assertEqual(len(requests_for_token), 1)
            body = requests_for_token[0].body
            assert body is not None
            self.assertEqual(
                json.loads(body),
                {
                    "grant_type": "authorization_code",
                    "code": "abc123",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "redirect_uri": f"{self.http_addr}/callback",
                },
            )

            requests_for_user = self.mock_oauth_server.requests[user_request]
            self.assertEqual(len(requests_for_user), 1)
            self.assertEqual(
                requests_for_user[0].headers["authorization"],
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

            pkce_object = await self.con.query(
                """
                SELECT ext::auth::PKCEChallenge
                { id, auth_token, refresh_token }
                filter .identity.id = <uuid>$identity_id
                """,
                identity_id=identity[0].id,
            )

            self.assertEqual(len(pkce_object), 1)
            self.assertEqual(pkce_object[0].auth_token, "github_access_token")
            self.assertIsNone(pkce_object[0].refresh_token)

            self.mock_oauth_server.register_route_handler(*user_request)(
                (
                    json.dumps(
                        {
                            "id": 1,
                            "login": "octocat",
                            "name": "monalisa octocat",
                            "email": "octocat+2@example.com",
                            "avatar_url": "https://example.com/example.jpg",
                            "updated_at": now.isoformat(),
                        }
                    ),
                    200,
                )
            )
            self.http_con_request(
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

    async def test_http_auth_ext_github_callback_failure_01(self):
        with self.http_con() as http_con:
            provider_config = await self.get_builtin_provider_config_by_name(
                "oauth_github"
            )
            provider_name = provider_config.name

            now = utcnow()
            token_request = (
                "POST",
                "https://github.com",
                "login/oauth/access_token",
            )
            self.mock_oauth_server.register_route_handler(*token_request)(
                (
                    json.dumps(
                        {
                            "access_token": "github_access_token",
                            "scope": "read:user",
                            "token_type": "bearer",
                        }
                    ),
                    200,
                )
            )

            signing_key = await self.get_signing_key()

            expires_at = now + datetime.timedelta(minutes=5)
            state_claims = {
                "iss": self.http_addr,
                "provider": str(provider_name),
                "exp": expires_at.timestamp(),
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
        with self.http_con() as http_con:
            provider_config = await self.get_builtin_provider_config_by_name(
                "oauth_github"
            )
            provider_name = provider_config.name

            now = utcnow()
            token_request = (
                "POST",
                "https://github.com",
                "login/oauth/access_token",
            )
            self.mock_oauth_server.register_route_handler(*token_request)(
                (
                    json.dumps(
                        {
                            "access_token": "github_access_token",
                            "scope": "read:user",
                            "token_type": "bearer",
                        }
                    ),
                    200,
                )
            )

            signing_key = await self.get_signing_key()

            expires_at = now + datetime.timedelta(minutes=5)
            state_claims = {
                "iss": self.http_addr,
                "provider": str(provider_name),
                "exp": expires_at.timestamp(),
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

    async def test_http_auth_ext_discord_authorize_01(self):
        with self.http_con() as http_con:
            provider_config = await self.get_builtin_provider_config_by_name(
                "oauth_discord"
            )
            provider_name = provider_config.name
            client_id = provider_config.client_id
            redirect_to = f"{self.http_addr}/some/path"
            challenge = (
                base64.urlsafe_b64encode(
                    hashlib.sha256(
                        base64.urlsafe_b64encode(os.urandom(43)).rstrip(b'=')
                    ).digest()
                )
                .rstrip(b'=')
                .decode()
            )
            query = {
                "provider": provider_name,
                "redirect_to": redirect_to,
                "code_challenge": challenge,
            }

            _, headers, status = self.http_con_request(
                http_con,
                query,
                path="authorize",
            )

            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            url = urllib.parse.urlparse(location)
            qs = urllib.parse.parse_qs(url.query, keep_blank_values=True)
            self.assertEqual(url.scheme, "https")
            self.assertEqual(url.hostname, "discord.com")
            self.assertEqual(url.path, "/oauth2/authorize")
            self.assertEqual(qs.get("scope"), ["email identify "])

            state = qs.get("state")
            assert state is not None

            claims = await self.extract_jwt_claims(state[0])
            self.assertEqual(claims.get("provider"), provider_name)
            self.assertEqual(claims.get("iss"), self.http_addr)
            self.assertEqual(claims.get("redirect_to"), redirect_to)

            self.assertEqual(
                qs.get("redirect_uri"), [f"{self.http_addr}/callback"]
            )
            self.assertEqual(qs.get("client_id"), [client_id])

            pkce = await self.con.query(
                """
                select ext::auth::PKCEChallenge
                filter .challenge = <str>$challenge
                """,
                challenge=challenge,
            )
            self.assertEqual(len(pkce), 1)

            _, _, repeat_status = self.http_con_request(
                http_con,
                query,
                path="authorize",
            )
            self.assertEqual(repeat_status, 302)

            repeat_pkce = await self.con.query_single(
                """
                select ext::auth::PKCEChallenge
                filter .challenge = <str>$challenge
                """,
                challenge=challenge,
            )
            self.assertEqual(pkce[0].id, repeat_pkce.id)

    async def test_http_auth_ext_discord_callback_01(self):
        with self.http_con() as http_con:
            provider_config = await self.get_builtin_provider_config_by_name(
                "oauth_discord"
            )
            provider_name = provider_config.name
            client_id = provider_config.client_id
            client_secret = DISCORD_SECRET

            now = utcnow()
            token_request = (
                "POST",
                "https://discord.com",
                "api/oauth2/token",
            )
            self.mock_oauth_server.register_route_handler(*token_request)(
                (
                    json.dumps(
                        {
                            "access_token": "discord_access_token",
                            "scope": "read:user",
                            "token_type": "bearer",
                        }
                    ),
                    200,
                )
            )

            user_request = ("GET", "https://discord.com", "api/v10/users/@me")
            self.mock_oauth_server.register_route_handler(*user_request)(
                (
                    json.dumps(
                        {
                            "id": 1,
                            "username": "dischord",
                            "global_name": "Ian MacKaye",
                            "email": "ian@example.com",
                            "picture": "https://example.com/example.jpg",
                        }
                    ),
                    200,
                )
            )

            challenge = (
                base64.urlsafe_b64encode(
                    hashlib.sha256(
                        base64.urlsafe_b64encode(os.urandom(43)).rstrip(b'=')
                    ).digest()
                )
                .rstrip(b'=')
                .decode()
            )
            await self.con.query(
                """
                insert ext::auth::PKCEChallenge {
                    challenge := <str>$challenge,
                }
                """,
                challenge=challenge,
            )

            signing_key = await self.get_signing_key()

            expires_at = now + datetime.timedelta(minutes=5)
            state_claims = {
                "iss": self.http_addr,
                "provider": str(provider_name),
                "exp": expires_at.timestamp(),
                "redirect_to": f"{self.http_addr}/some/path",
                "challenge": challenge,
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

            requests_for_token = self.mock_oauth_server.requests[token_request]
            self.assertEqual(len(requests_for_token), 1)

            self.assertEqual(
                urllib.parse.parse_qs(requests_for_token[0].body),
                {
                    "grant_type": ["authorization_code"],
                    "code": ["abc123"],
                    "client_id": [client_id],
                    "client_secret": [client_secret],
                    "redirect_uri": [f"{self.http_addr}/callback"],
                },
            )

            requests_for_user = self.mock_oauth_server.requests[user_request]
            self.assertEqual(len(requests_for_user), 1)
            self.assertEqual(
                requests_for_user[0].headers["authorization"],
                "Bearer discord_access_token",
            )

            identity = await self.con.query(
                """
                SELECT ext::auth::Identity
                FILTER .subject = '1'
                AND .issuer = 'https://discord.com'
                """
            )
            self.assertEqual(len(identity), 1)

            pkce_object = await self.con.query(
                """
                SELECT ext::auth::PKCEChallenge
                { id, auth_token, refresh_token }
                filter .identity.id = <uuid>$identity_id
                """,
                identity_id=identity[0].id,
            )

            self.assertEqual(len(pkce_object), 1)
            self.assertEqual(pkce_object[0].auth_token, "discord_access_token")
            self.assertIsNone(pkce_object[0].refresh_token)

            self.mock_oauth_server.register_route_handler(*user_request)(
                (
                    json.dumps(
                        {
                            "id": 1,
                            "login": "octocat",
                            "name": "monalisa octocat",
                            "email": "octocat+2@example.com",
                            "avatar_url": "https://example.com/example.jpg",
                            "updated_at": now.isoformat(),
                        }
                    ),
                    200,
                )
            )
            self.http_con_request(
                http_con,
                {"state": state_token, "code": "abc123"},
                path="callback",
            )

            same_identity = await self.con.query(
                """
                SELECT ext::auth::Identity
                FILTER .subject = '1'
                AND .issuer = 'https://discord.com'
                """
            )
            self.assertEqual(len(same_identity), 1)
            self.assertEqual(identity[0].id, same_identity[0].id)

    async def test_http_auth_ext_google_callback_01(self) -> None:
        with self.http_con() as http_con:
            provider_config = await self.get_builtin_provider_config_by_name(
                "oauth_google"
            )
            provider_name = provider_config.name
            client_id = provider_config.client_id
            client_secret = GOOGLE_SECRET

            now = utcnow()

            discovery_request = (
                "GET",
                "https://accounts.google.com",
                ".well-known/openid-configuration",
            )
            self.mock_oauth_server.register_route_handler(*discovery_request)(
                (
                    json.dumps(GOOGLE_DISCOVERY_DOCUMENT),
                    200,
                    {"cache-control": "max-age=3600"},
                )
            )

            jwks_request = (
                "GET",
                "https://www.googleapis.com",
                "oauth2/v3/certs",
            )
            # Generate a JWK Set
            k = jwk.JWK.generate(kty='RSA', size=4096)
            ks = jwk.JWKSet()
            ks.add(k)
            jwk_set: dict[str, Any] = ks.export(
                private_keys=False, as_dict=True
            )

            self.mock_oauth_server.register_route_handler(*jwks_request)(
                (
                    json.dumps(jwk_set),
                    200,
                )
            )

            token_request = (
                "POST",
                "https://oauth2.googleapis.com",
                "token",
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

            self.mock_oauth_server.register_route_handler(*token_request)(
                (
                    json.dumps(
                        {
                            "access_token": "google_access_token",
                            "id_token": id_token.serialize(),
                            "scope": "openid",
                            "token_type": "bearer",
                        }
                    ),
                    200,
                )
            )

            challenge = (
                base64.urlsafe_b64encode(
                    hashlib.sha256(
                        base64.urlsafe_b64encode(os.urandom(43)).rstrip(b'=')
                    ).digest()
                )
                .rstrip(b'=')
                .decode()
            )
            await self.con.query(
                """
                insert ext::auth::PKCEChallenge {
                    challenge := <str>$challenge,
                }
                """,
                challenge=challenge,
            )

            signing_key = await self.get_signing_key()

            expires_at = now + datetime.timedelta(minutes=5)
            state_claims = {
                "iss": self.http_addr,
                "provider": str(provider_name),
                "exp": expires_at.timestamp(),
                "redirect_to": f"{self.http_addr}/some/path",
                "challenge": challenge,
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

            requests_for_discovery = self.mock_oauth_server.requests[
                discovery_request
            ]
            self.assertEqual(len(requests_for_discovery), 2)

            requests_for_token = self.mock_oauth_server.requests[token_request]
            self.assertEqual(len(requests_for_token), 1)
            self.assertEqual(
                urllib.parse.parse_qs(requests_for_token[0].body),
                {
                    "grant_type": ["authorization_code"],
                    "code": ["abc123"],
                    "client_id": [client_id],
                    "client_secret": [client_secret],
                    "redirect_uri": [f"{self.http_addr}/callback"],
                },
            )

            identity = await self.con.query(
                """
                SELECT ext::auth::Identity
                FILTER .subject = '1'
                AND .issuer = 'https://accounts.google.com'
                """
            )
            self.assertEqual(len(identity), 1)

    async def test_http_auth_ext_google_authorize_01(self):
        with self.http_con() as http_con:
            provider_config = await self.get_builtin_provider_config_by_name(
                "oauth_google"
            )
            provider_name = provider_config.name
            client_id = provider_config.client_id
            challenge = (
                base64.urlsafe_b64encode(
                    hashlib.sha256(
                        base64.urlsafe_b64encode(os.urandom(43)).rstrip(b'=')
                    ).digest()
                )
                .rstrip(b'=')
                .decode()
            )

            discovery_request = (
                "GET",
                "https://accounts.google.com",
                ".well-known/openid-configuration",
            )
            self.mock_oauth_server.register_route_handler(*discovery_request)(
                (
                    json.dumps(GOOGLE_DISCOVERY_DOCUMENT),
                    200,
                )
            )

            redirect_to = f"{self.http_addr}/some/path"
            _, headers, status = self.http_con_request(
                http_con,
                {
                    "provider": provider_name,
                    "redirect_to": redirect_to,
                    "challenge": challenge,
                },
                path="authorize",
            )

            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            url = urllib.parse.urlparse(location)
            qs = urllib.parse.parse_qs(url.query, keep_blank_values=True)
            self.assertEqual(url.scheme, "https")
            self.assertEqual(url.hostname, "accounts.google.com")
            self.assertEqual(url.path, "/o/oauth2/v2/auth")
            self.assertEqual(qs.get("scope"), ["openid profile email "])

            state = qs.get("state")
            assert state is not None

            claims = await self.extract_jwt_claims(state[0])
            self.assertEqual(claims.get("provider"), provider_name)
            self.assertEqual(claims.get("iss"), self.http_addr)
            self.assertEqual(claims.get("redirect_to"), redirect_to)

            self.assertEqual(
                qs.get("redirect_uri"), [f"{self.http_addr}/callback"]
            )
            self.assertEqual(qs.get("client_id"), [client_id])

            requests_for_discovery = self.mock_oauth_server.requests[
                discovery_request
            ]
            self.assertEqual(len(requests_for_discovery), 1)

            pkce = await self.con.query(
                """
                select ext::auth::PKCEChallenge
                filter .challenge = <str>$challenge
                """,
                challenge=challenge,
            )
            self.assertEqual(len(pkce), 1)

    async def test_http_auth_ext_azure_authorize_01(self):
        with self.http_con() as http_con:
            provider_config = await self.get_builtin_provider_config_by_name(
                "oauth_azure"
            )
            provider_name = provider_config.name
            client_id = provider_config.client_id
            challenge = "a" * 32

            discovery_request = (
                "GET",
                "https://login.microsoftonline.com",
                "common/v2.0/.well-known/openid-configuration",
            )
            self.mock_oauth_server.register_route_handler(*discovery_request)(
                (
                    json.dumps(AZURE_DISCOVERY_DOCUMENT),
                    200,
                )
            )

            redirect_to = f"{self.http_addr}/some/path"
            _, headers, status = self.http_con_request(
                http_con,
                {
                    "provider": provider_name,
                    "redirect_to": redirect_to,
                    "challenge": challenge,
                },
                path="authorize",
            )

            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            url = urllib.parse.urlparse(location)
            qs = urllib.parse.parse_qs(url.query, keep_blank_values=True)
            self.assertEqual(url.scheme, "https")
            self.assertEqual(url.hostname, "login.microsoftonline.com")
            self.assertEqual(url.path, "/common/oauth2/v2.0/authorize")
            self.assertEqual(
                qs.get("scope"), ["openid profile email offline_access"]
            )

            state = qs.get("state")
            assert state is not None

            claims = await self.extract_jwt_claims(state[0])
            self.assertEqual(claims.get("provider"), provider_name)
            self.assertEqual(claims.get("iss"), self.http_addr)
            self.assertEqual(claims.get("redirect_to"), redirect_to)

            self.assertEqual(
                qs.get("redirect_uri"), [f"{self.http_addr}/callback"]
            )
            self.assertEqual(qs.get("client_id"), [client_id])

            requests_for_discovery = self.mock_oauth_server.requests[
                discovery_request
            ]
            self.assertEqual(len(requests_for_discovery), 1)

            pkce = await self.con.query(
                """
                select ext::auth::PKCEChallenge
                filter .challenge = <str>$challenge
                """,
                challenge=challenge,
            )
            self.assertEqual(len(pkce), 1)

    async def test_http_auth_ext_azure_callback_01(self) -> None:
        with self.http_con() as http_con:
            provider_config = await self.get_builtin_provider_config_by_name(
                "oauth_azure"
            )
            provider_name = provider_config.name
            client_id = provider_config.client_id
            client_secret = AZURE_SECRET

            now = utcnow()

            discovery_request = (
                "GET",
                "https://login.microsoftonline.com",
                "common/v2.0/.well-known/openid-configuration",
            )
            self.mock_oauth_server.register_route_handler(*discovery_request)(
                (
                    json.dumps(AZURE_DISCOVERY_DOCUMENT),
                    200,
                )
            )

            jwks_request = (
                "GET",
                "https://login.microsoftonline.com",
                "common/discovery/v2.0/keys",
            )
            # Generate a JWK Set
            k = jwk.JWK.generate(kty='RSA', size=4096)
            ks = jwk.JWKSet()
            ks.add(k)
            jwk_set: dict[str, Any] = ks.export(
                private_keys=False, as_dict=True
            )

            self.mock_oauth_server.register_route_handler(*jwks_request)(
                (
                    json.dumps(jwk_set),
                    200,
                )
            )

            token_request = (
                "POST",
                "https://login.microsoftonline.com",
                "common/oauth2/v2.0/token",
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

            self.mock_oauth_server.register_route_handler(*token_request)(
                (
                    json.dumps(
                        {
                            "access_token": "azure_access_token",
                            "id_token": id_token.serialize(),
                            "scope": "openid",
                            "token_type": "bearer",
                        }
                    ),
                    200,
                )
            )

            challenge = (
                base64.urlsafe_b64encode(
                    hashlib.sha256(
                        base64.urlsafe_b64encode(os.urandom(43)).rstrip(b'=')
                    ).digest()
                )
                .rstrip(b'=')
                .decode()
            )
            await self.con.query(
                """
                insert ext::auth::PKCEChallenge {
                    challenge := <str>$challenge,
                }
                """,
                challenge=challenge,
            )

            signing_key = await self.get_signing_key()

            expires_at = now + datetime.timedelta(minutes=5)
            state_claims = {
                "iss": self.http_addr,
                "provider": str(provider_name),
                "exp": expires_at.timestamp(),
                "redirect_to": f"{self.http_addr}/some/path",
                "challenge": challenge,
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

            requests_for_discovery = self.mock_oauth_server.requests[
                discovery_request
            ]
            self.assertEqual(len(requests_for_discovery), 2)

            requests_for_token = self.mock_oauth_server.requests[token_request]
            self.assertEqual(len(requests_for_token), 1)
            self.assertEqual(
                urllib.parse.parse_qs(requests_for_token[0].body),
                {
                    "grant_type": ["authorization_code"],
                    "code": ["abc123"],
                    "client_id": [client_id],
                    "client_secret": [client_secret],
                    "redirect_uri": [f"{self.http_addr}/callback"],
                },
            )

    async def test_http_auth_ext_apple_authorize_01(self):
        with self.http_con() as http_con:
            provider_config = await self.get_builtin_provider_config_by_name(
                "oauth_apple"
            )
            provider_name = provider_config.name
            client_id = provider_config.client_id
            challenge = (
                base64.urlsafe_b64encode(
                    hashlib.sha256(
                        base64.urlsafe_b64encode(os.urandom(43)).rstrip(b'=')
                    ).digest()
                )
                .rstrip(b'=')
                .decode()
            )

            discovery_request = (
                "GET",
                "https://appleid.apple.com",
                ".well-known/openid-configuration",
            )
            self.mock_oauth_server.register_route_handler(*discovery_request)(
                (
                    json.dumps(APPLE_DISCOVERY_DOCUMENT),
                    200,
                )
            )

            redirect_to = f"{self.http_addr}/some/path"
            _, headers, status = self.http_con_request(
                http_con,
                {
                    "provider": provider_name,
                    "redirect_to": redirect_to,
                    "challenge": challenge,
                },
                path="authorize",
            )

            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            url = urllib.parse.urlparse(location)
            qs = urllib.parse.parse_qs(url.query, keep_blank_values=True)
            self.assertEqual(url.scheme, "https")
            self.assertEqual(url.hostname, "appleid.apple.com")
            self.assertEqual(url.path, "/auth/authorize")
            self.assertEqual(qs.get("scope"), ["openid email name "])

            state = qs.get("state")
            assert state is not None

            claims = await self.extract_jwt_claims(state[0])
            self.assertEqual(claims.get("provider"), provider_name)
            self.assertEqual(claims.get("iss"), self.http_addr)
            self.assertEqual(claims.get("redirect_to"), redirect_to)

            self.assertEqual(
                qs.get("redirect_uri"), [f"{self.http_addr}/callback"]
            )
            self.assertEqual(qs.get("client_id"), [client_id])

            requests_for_discovery = self.mock_oauth_server.requests[
                discovery_request
            ]
            self.assertEqual(len(requests_for_discovery), 1)

            pkce = await self.con.query(
                """
                select ext::auth::PKCEChallenge
                filter .challenge = <str>$challenge
                """,
                challenge=challenge,
            )
            self.assertEqual(len(pkce), 1)

    async def test_http_auth_ext_apple_callback_01(self) -> None:
        with self.http_con() as http_con:
            provider_config = await self.get_builtin_provider_config_by_name(
                "oauth_apple"
            )
            provider_name = provider_config.name
            client_id = provider_config.client_id
            client_secret = APPLE_SECRET

            now = utcnow()

            discovery_request = (
                "GET",
                "https://appleid.apple.com",
                ".well-known/openid-configuration",
            )
            self.mock_oauth_server.register_route_handler(*discovery_request)(
                (
                    json.dumps(APPLE_DISCOVERY_DOCUMENT),
                    200,
                )
            )

            jwks_request = (
                "GET",
                "https://appleid.apple.com",
                "auth/keys",
            )
            # Generate a JWK Set
            k = jwk.JWK.generate(kty='RSA', size=4096)
            ks = jwk.JWKSet()
            ks.add(k)
            jwk_set: dict[str, Any] = ks.export(
                private_keys=False, as_dict=True
            )

            self.mock_oauth_server.register_route_handler(*jwks_request)(
                (
                    json.dumps(jwk_set),
                    200,
                )
            )

            token_request = (
                "POST",
                "https://appleid.apple.com",
                "auth/token",
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

            self.mock_oauth_server.register_route_handler(*token_request)(
                (
                    json.dumps(
                        {
                            "access_token": "apple_access_token",
                            "id_token": id_token.serialize(),
                            "scope": "openid",
                            "token_type": "bearer",
                        }
                    ),
                    200,
                )
            )

            challenge = (
                base64.urlsafe_b64encode(
                    hashlib.sha256(
                        base64.urlsafe_b64encode(os.urandom(43)).rstrip(b'=')
                    ).digest()
                )
                .rstrip(b'=')
                .decode()
            )
            await self.con.query(
                """
                insert ext::auth::PKCEChallenge {
                    challenge := <str>$challenge,
                }
                """,
                challenge=challenge,
            )

            signing_key = await self.get_signing_key()

            expires_at = now + datetime.timedelta(minutes=5)
            state_claims = {
                "iss": self.http_addr,
                "provider": str(provider_name),
                "exp": expires_at.timestamp(),
                "redirect_to": f"{self.http_addr}/some/path",
                "challenge": challenge,
            }
            state_token = self.generate_state_value(state_claims, signing_key)

            data, headers, status = self.http_con_request(
                http_con,
                None,
                path="callback",
                method="POST",
                body=urllib.parse.urlencode(
                    {"state": state_token, "code": "abc123"}
                ).encode(),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
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

            requests_for_discovery = self.mock_oauth_server.requests[
                discovery_request
            ]
            self.assertEqual(len(requests_for_discovery), 2)

            requests_for_token = self.mock_oauth_server.requests[token_request]
            self.assertEqual(len(requests_for_token), 1)
            self.assertEqual(
                urllib.parse.parse_qs(requests_for_token[0].body),
                {
                    "grant_type": ["authorization_code"],
                    "code": ["abc123"],
                    "client_id": [client_id],
                    "client_secret": [client_secret],
                    "redirect_uri": [f"{self.http_addr}/callback"],
                },
            )

    async def test_http_auth_ext_apple_callback_redirect_on_signup_02(
        self,
    ) -> None:
        with self.http_con() as http_con:
            provider_config = await self.get_builtin_provider_config_by_name(
                "oauth_apple"
            )
            provider_name = provider_config.name
            client_id = provider_config.client_id

            now = utcnow()

            discovery_request = (
                "GET",
                "https://appleid.apple.com",
                ".well-known/openid-configuration",
            )
            self.mock_oauth_server.register_route_handler(*discovery_request)(
                (
                    json.dumps(APPLE_DISCOVERY_DOCUMENT),
                    200,
                )
            )

            jwks_request = (
                "GET",
                "https://appleid.apple.com",
                "auth/keys",
            )
            # Generate a JWK Set
            k = jwk.JWK.generate(kty='RSA', size=4096)
            ks = jwk.JWKSet()
            ks.add(k)
            jwk_set: dict[str, Any] = ks.export(
                private_keys=False, as_dict=True
            )

            self.mock_oauth_server.register_route_handler(*jwks_request)(
                (
                    json.dumps(jwk_set),
                    200,
                )
            )

            token_request = (
                "POST",
                "https://appleid.apple.com",
                "auth/token",
            )
            id_token_claims = {
                "iss": "https://appleid.apple.com",
                "sub": "2",
                "aud": client_id,
                "exp": (now + datetime.timedelta(minutes=5)).timestamp(),
                "iat": now.timestamp(),
                "email": "test@example.com",
            }
            id_token = jwt.JWT(header={"alg": "RS256"}, claims=id_token_claims)
            id_token.make_signed_token(k)

            self.mock_oauth_server.register_route_handler(*token_request)(
                (
                    json.dumps(
                        {
                            "access_token": "apple_access_token",
                            "id_token": id_token.serialize(),
                            "scope": "openid",
                            "token_type": "bearer",
                        }
                    ),
                    200,
                )
            )

            challenge = (
                base64.urlsafe_b64encode(
                    hashlib.sha256(
                        base64.urlsafe_b64encode(os.urandom(43)).rstrip(b'=')
                    ).digest()
                )
                .rstrip(b'=')
                .decode()
            )
            await self.con.query(
                """
                insert ext::auth::PKCEChallenge {
                    challenge := <str>$challenge,
                }
                """,
                challenge=challenge,
            )

            signing_key = await self.get_signing_key()

            expires_at = now + datetime.timedelta(minutes=5)
            state_claims = {
                "iss": self.http_addr,
                "provider": str(provider_name),
                "exp": expires_at.timestamp(),
                "redirect_to": f"{self.http_addr}/some/path",
                "redirect_to_on_signup": f"{self.http_addr}/some/other/path",
                "challenge": challenge,
            }
            state_token = self.generate_state_value(state_claims, signing_key)

            data, headers, status = self.http_con_request(
                http_con,
                None,
                path="callback",
                method="POST",
                body=urllib.parse.urlencode(
                    {"state": state_token, "code": "abc123"}
                ).encode(),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(data, b"")
            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            server_url = urllib.parse.urlparse(self.http_addr)
            url = urllib.parse.urlparse(location)
            self.assertEqual(url.scheme, server_url.scheme)
            self.assertEqual(url.hostname, server_url.hostname)
            self.assertEqual(url.path, f"{server_url.path}/some/other/path")

            data, headers, status = self.http_con_request(
                http_con,
                None,
                path="callback",
                method="POST",
                body=urllib.parse.urlencode(
                    {"state": state_token, "code": "abc123"}
                ).encode(),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
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

    async def test_http_auth_ext_slack_callback_01(self) -> None:
        with self.http_con() as http_con:
            provider_config = await self.get_builtin_provider_config_by_name(
                "oauth_slack"
            )
            provider_name = provider_config.name
            client_id = provider_config.client_id
            client_secret = SLACK_SECRET

            now = utcnow()

            discovery_request = (
                "GET",
                "https://slack.com",
                ".well-known/openid-configuration",
            )
            self.mock_oauth_server.register_route_handler(*discovery_request)(
                (
                    json.dumps(SLACK_DISCOVERY_DOCUMENT),
                    200,
                    {"cache-control": "max-age=3600"},
                )
            )

            jwks_request = (
                "GET",
                "https://slack.com",
                "openid/connect/keys",
            )
            # Generate a JWK Set
            k = jwk.JWK.generate(kty='RSA', size=4096)
            ks = jwk.JWKSet()
            ks.add(k)
            jwk_set: dict[str, Any] = ks.export(
                private_keys=False, as_dict=True
            )

            self.mock_oauth_server.register_route_handler(*jwks_request)(
                (
                    json.dumps(jwk_set),
                    200,
                )
            )

            token_request = (
                "POST",
                "https://slack.com",
                "api/openid.connect.token",
            )
            id_token_claims = {
                "iss": "https://slack.com",
                "sub": "1",
                "aud": client_id,
                "exp": (now + datetime.timedelta(minutes=5)).timestamp(),
                "iat": now.timestamp(),
                "email": "test@example.com",
            }
            id_token = jwt.JWT(header={"alg": "RS256"}, claims=id_token_claims)
            id_token.make_signed_token(k)

            self.mock_oauth_server.register_route_handler(*token_request)(
                (
                    json.dumps(
                        {
                            "access_token": "slack_access_token",
                            "id_token": id_token.serialize(),
                            "scope": "openid",
                            "token_type": "bearer",
                        }
                    ),
                    200,
                )
            )

            challenge = (
                base64.urlsafe_b64encode(
                    hashlib.sha256(
                        base64.urlsafe_b64encode(os.urandom(43)).rstrip(b'=')
                    ).digest()
                )
                .rstrip(b'=')
                .decode()
            )
            await self.con.query(
                """
                insert ext::auth::PKCEChallenge {
                    challenge := <str>$challenge,
                }
                """,
                challenge=challenge,
            )

            signing_key = await self.get_signing_key()

            expires_at = now + datetime.timedelta(minutes=5)
            state_claims = {
                "iss": self.http_addr,
                "provider": str(provider_name),
                "exp": expires_at.timestamp(),
                "redirect_to": f"{self.http_addr}/some/path",
                "challenge": challenge,
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

            requests_for_discovery = self.mock_oauth_server.requests[
                discovery_request
            ]
            self.assertEqual(len(requests_for_discovery), 2)

            requests_for_token = self.mock_oauth_server.requests[token_request]
            self.assertEqual(len(requests_for_token), 1)
            self.assertEqual(
                urllib.parse.parse_qs(requests_for_token[0].body),
                {
                    "grant_type": ["authorization_code"],
                    "code": ["abc123"],
                    "client_id": [client_id],
                    "client_secret": [client_secret],
                    "redirect_uri": [f"{self.http_addr}/callback"],
                },
            )

            identity = await self.con.query(
                """
                SELECT ext::auth::Identity
                FILTER .subject = '1'
                AND .issuer = 'https://slack.com'
                """
            )
            self.assertEqual(len(identity), 1)

    async def test_http_auth_ext_slack_authorize_01(self):
        with self.http_con() as http_con:
            provider_config = await self.get_builtin_provider_config_by_name(
                "oauth_slack"
            )
            provider_name = provider_config.name
            client_id = provider_config.client_id
            challenge = (
                base64.urlsafe_b64encode(
                    hashlib.sha256(
                        base64.urlsafe_b64encode(os.urandom(43)).rstrip(b'=')
                    ).digest()
                )
                .rstrip(b'=')
                .decode()
            )

            discovery_request = (
                "GET",
                "https://slack.com",
                ".well-known/openid-configuration",
            )
            self.mock_oauth_server.register_route_handler(*discovery_request)(
                (
                    json.dumps(SLACK_DISCOVERY_DOCUMENT),
                    200,
                )
            )

            redirect_to = f"{self.http_addr}/some/path"
            _, headers, status = self.http_con_request(
                http_con,
                {
                    "provider": provider_name,
                    "redirect_to": redirect_to,
                    "challenge": challenge,
                },
                path="authorize",
            )

            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            url = urllib.parse.urlparse(location)
            qs = urllib.parse.parse_qs(url.query, keep_blank_values=True)
            self.assertEqual(url.scheme, "https")
            self.assertEqual(url.hostname, "slack.com")
            self.assertEqual(url.path, "/openid/connect/authorize")
            self.assertEqual(qs.get("scope"), ["openid profile email "])

            state = qs.get("state")
            assert state is not None

            claims = await self.extract_jwt_claims(state[0])
            self.assertEqual(claims.get("provider"), provider_name)
            self.assertEqual(claims.get("iss"), self.http_addr)
            self.assertEqual(claims.get("redirect_to"), redirect_to)

            self.assertEqual(
                qs.get("redirect_uri"), [f"{self.http_addr}/callback"]
            )
            self.assertEqual(qs.get("client_id"), [client_id])

            requests_for_discovery = self.mock_oauth_server.requests[
                discovery_request
            ]
            self.assertEqual(len(requests_for_discovery), 1)

            pkce = await self.con.query(
                """
                select ext::auth::PKCEChallenge
                filter .challenge = <str>$challenge
                """,
                challenge=challenge,
            )
            self.assertEqual(len(pkce), 1)

    async def test_http_auth_ext_generic_oidc_authorize_01(self):
        with self.http_con() as http_con:
            provider_config = await self.get_provider_config_by_name(
                "generic_oidc"
            )
            provider_name = provider_config.name
            client_id = provider_config.client_id
            challenge = (
                base64.urlsafe_b64encode(
                    hashlib.sha256(
                        base64.urlsafe_b64encode(os.urandom(43)).rstrip(b'=')
                    ).digest()
                )
                .rstrip(b'=')
                .decode()
            )

            discovery_request = (
                "GET",
                "https://example.com",
                ".well-known/openid-configuration",
            )
            self.mock_oauth_server.register_route_handler(*discovery_request)(
                (
                    json.dumps(GENERIC_OIDC_DISCOVERY_DOCUMENT),
                    200,
                )
            )

            redirect_to = f"{self.http_addr}/some/path"
            _, headers, status = self.http_con_request(
                http_con,
                {
                    "provider": provider_name,
                    "redirect_to": redirect_to,
                    "challenge": challenge,
                },
                path="authorize",
            )

            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            url = urllib.parse.urlparse(location)
            qs = urllib.parse.parse_qs(url.query, keep_blank_values=True)
            self.assertEqual(url.scheme, "https")
            self.assertEqual(url.hostname, "example.com")
            self.assertEqual(url.path, "/auth")
            self.assertEqual(
                qs.get("scope"),
                ["openid profile email custom_provider_scope_string"],
            )

            state = qs.get("state")
            assert state is not None

            claims = await self.extract_jwt_claims(state[0])
            self.assertEqual(claims.get("provider"), provider_name)
            self.assertEqual(claims.get("iss"), self.http_addr)
            self.assertEqual(claims.get("redirect_to"), redirect_to)

            self.assertEqual(
                qs.get("redirect_uri"), [f"{self.http_addr}/callback"]
            )
            self.assertEqual(qs.get("client_id"), [client_id])

            requests_for_discovery = self.mock_oauth_server.requests[
                discovery_request
            ]
            self.assertEqual(len(requests_for_discovery), 1)

            pkce = await self.con.query(
                """
                select ext::auth::PKCEChallenge
                filter .challenge = <str>$challenge
                """,
                challenge=challenge,
            )
            self.assertEqual(len(pkce), 1)

    async def test_http_auth_ext_generic_oidc_callback_01(self):
        with self.http_con() as http_con:
            provider_config = await self.get_provider_config_by_name(
                "generic_oidc"
            )
            provider_name = provider_config.name
            client_id = provider_config.client_id
            client_secret = GENERIC_OIDC_SECRET

            now = utcnow()

            discovery_request = (
                "GET",
                "https://example.com",
                ".well-known/openid-configuration",
            )
            self.mock_oauth_server.register_route_handler(*discovery_request)(
                (
                    json.dumps(GENERIC_OIDC_DISCOVERY_DOCUMENT),
                    200,
                    {"cache-control": "max-age=3600"},
                )
            )

            jwks_request = (
                "GET",
                "https://example.com",
                "jwks",
            )
            # Generate a JWK Set
            k = jwk.JWK.generate(kty='RSA', size=4096)
            ks = jwk.JWKSet()
            ks.add(k)
            jwk_set: dict[str, Any] = ks.export(
                private_keys=False, as_dict=True
            )

            self.mock_oauth_server.register_route_handler(*jwks_request)(
                (
                    json.dumps(jwk_set),
                    200,
                )
            )

            token_request = (
                "POST",
                "https://example.com",
                "token",
            )
            id_token_claims = {
                "iss": "https://example.com",
                "sub": "1",
                "aud": client_id,
                "exp": (now + datetime.timedelta(minutes=5)).timestamp(),
                "iat": now.timestamp(),
                "email": "test@example.com",
            }
            id_token = jwt.JWT(header={"alg": "RS256"}, claims=id_token_claims)
            id_token.make_signed_token(k)

            self.mock_oauth_server.register_route_handler(*token_request)(
                (
                    json.dumps(
                        {
                            "access_token": "oidc_access_token",
                            "id_token": id_token.serialize(),
                            "scope": "openid",
                            "token_type": "bearer",
                        }
                    ),
                    200,
                )
            )

            challenge = (
                base64.urlsafe_b64encode(
                    hashlib.sha256(
                        base64.urlsafe_b64encode(os.urandom(43)).rstrip(b'=')
                    ).digest()
                )
                .rstrip(b'=')
                .decode()
            )
            await self.con.query(
                """
                insert ext::auth::PKCEChallenge {
                    challenge := <str>$challenge,
                }
                """,
                challenge=challenge,
            )

            signing_key = await self.get_signing_key()

            expires_at = now + datetime.timedelta(minutes=5)
            state_claims = {
                "iss": self.http_addr,
                "provider": str(provider_name),
                "exp": expires_at.timestamp(),
                "redirect_to": f"{self.http_addr}/some/path",
                "challenge": challenge,
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

            requests_for_discovery = self.mock_oauth_server.requests[
                discovery_request
            ]
            self.assertEqual(len(requests_for_discovery), 2)

            requests_for_token = self.mock_oauth_server.requests[token_request]
            self.assertEqual(len(requests_for_token), 1)
            self.assertEqual(
                urllib.parse.parse_qs(requests_for_token[0].body),
                {
                    "grant_type": ["authorization_code"],
                    "code": ["abc123"],
                    "client_id": [client_id],
                    "client_secret": [client_secret],
                    "redirect_uri": [f"{self.http_addr}/callback"],
                },
            )

            identity = await self.con.query(
                """
                SELECT ext::auth::Identity
                FILTER .subject = '1'
                AND .issuer = 'https://example.com'
                """
            )
            self.assertEqual(len(identity), 1)

    async def test_http_auth_ext_local_password_register_form_01(self):
        base_url = self.mock_net_server.get_base_url().rstrip("/")
        url = f"{base_url}/webhook-01"
        alt_url = f"{base_url}/webhook-03"
        await self.con.query(
            """
            CONFIGURE CURRENT DATABASE
            INSERT ext::auth::WebhookConfig {
                url := <str>$url,
                events := {
                    ext::auth::WebhookEvent.IdentityCreated,
                    ext::auth::WebhookEvent.EmailFactorCreated,
                    ext::auth::WebhookEvent.EmailVerificationRequested,
                },
            };
            """,
            url=url,
        )
        await self.con.query(
            """
            CONFIGURE CURRENT DATABASE
            INSERT ext::auth::WebhookConfig {
                url := <str>$alt_url,
                events := {
                    ext::auth::WebhookEvent.IdentityCreated,
                },
            };
            """,
            alt_url=alt_url,
        )
        webhook_request = (
            "POST",
            base_url,
            "/webhook-01",
        )
        alt_webhook_request = (
            "POST",
            base_url,
            "/webhook-03",
        )
        await self._wait_for_db_config("ext::auth::AuthConfig::webhooks")
        try:
            with self.http_con() as http_con:
                self.mock_net_server.register_route_handler(*webhook_request)(
                    (
                        "",
                        204,
                    )
                )
                self.mock_net_server.register_route_handler(
                    *alt_webhook_request
                )(
                    (
                        "",
                        204,
                    )
                )
                provider_config = (
                    await self.get_builtin_provider_config_by_name(
                        "local_emailpassword"
                    )
                )
                provider_name = provider_config.name
                email = f"{uuid.uuid4()}@example.com"

                form_data = {
                    "provider": provider_name,
                    "email": email,
                    "password": "test_password",
                    "redirect_to": "https://oauth.example.com/app/path",
                    "challenge": str(uuid.uuid4()),
                }
                form_data_encoded = urllib.parse.urlencode(form_data).encode()

                _, headers, status = self.http_con_request(
                    http_con,
                    None,
                    path="register",
                    method="POST",
                    body=form_data_encoded,
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded"
                    },
                )

                identity = await self.con.query(
                    """
                    SELECT ext::auth::LocalIdentity
                    FILTER .<identity[is ext::auth::EmailPasswordFactor]
                        .email = <str>$email;
                    """,
                    email=email,
                )

                self.assertEqual(len(identity), 1)

                pkce_challenge = await self.con.query_required_single(
                    """
                    SELECT ext::auth::PKCEChallenge { * }
                    FILTER .challenge = <str>$challenge
                    AND .identity.id = <uuid>$identity_id;
                    """,
                    challenge=form_data["challenge"],
                    identity_id=identity[0].id,
                )

                self.assertEqual(status, 302)
                location = headers.get("location")
                assert location is not None
                parsed_location = urllib.parse.urlparse(location)
                parsed_query = urllib.parse.parse_qs(parsed_location.query)
                self.assertEqual(parsed_location.scheme, "https")
                self.assertEqual(parsed_location.netloc, "oauth.example.com")
                self.assertEqual(parsed_location.path, "/app/path")
                self.assertEqual(
                    parsed_query,
                    {
                        "code": [str(pkce_challenge.id)],
                        "provider": ["builtin::local_emailpassword"],
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
                        password_credential[0].password_hash, "test_password"
                    )
                )

                # Test Webhook side effect
                async for tr in self.try_until_succeeds(
                    delay=2, timeout=120, ignore=(KeyError, AssertionError)
                ):
                    async with tr:
                        requests_for_webhook = self.mock_net_server.requests[
                            webhook_request
                        ]
                        self.assertEqual(len(requests_for_webhook), 3)

                event_types: dict[str, dict | None] = {
                    "IdentityCreated": None,
                    "EmailFactorCreated": None,
                    "EmailVerificationRequested": None,
                }
                for request in requests_for_webhook:
                    assert request.body is not None
                    event_data = json.loads(request.body)
                    event_type = event_data["event_type"]
                    self.assertIn(event_type, event_types)
                    self.assertEqual(
                        event_data["identity_id"], str(identity[0].id)
                    )
                    event_types[event_type] = event_data

                self.assertTrue(
                    all(value is not None for value in event_types.values())
                )
                self.assertIn(
                    "verification_token",
                    cast(dict, event_types["EmailVerificationRequested"]),
                )

                # Test for alt_url webhook
                async for tr in self.try_until_succeeds(
                    delay=2, timeout=120, ignore=(KeyError, AssertionError)
                ):
                    async with tr:
                        requests_for_alt_webhook = (
                            self.mock_net_server.requests[alt_webhook_request]
                        )
                        self.assertEqual(len(requests_for_alt_webhook), 1)

                # Try to register the same user again (no redirect_to)
                _, _, conflict_status = self.http_con_request(
                    http_con,
                    None,
                    path="register",
                    method="POST",
                    body=urllib.parse.urlencode(
                        {
                            **{
                                k: v
                                for k, v in form_data.items()
                                if k != 'redirect_to'
                            },
                            "challenge": str(uuid.uuid4()),
                        }
                    ).encode(),
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded"
                    },
                )

                self.assertEqual(conflict_status, 409)

                # Try to register the same user again (no redirect_on_failure)
                _, redirect_to_headers, redirect_to_status = (
                    self.http_con_request(
                        http_con,
                        None,
                        path="register",
                        method="POST",
                        body=urllib.parse.urlencode(
                            {
                                **form_data,
                                "challenge": str(uuid.uuid4()),
                            }
                        ).encode(),
                        headers={
                            "Content-Type": "application/x-www-form-urlencoded"
                        },
                    )
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
                redirect_on_failure_url = (
                    "https://example.com/app/path/different"
                )
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
                            "challenge": str(uuid.uuid4()),
                        }
                    ).encode(),
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded"
                    },
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
        finally:
            await self.con.query(
                """
                CONFIGURE CURRENT DATABASE
                RESET ext::auth::WebhookConfig
                filter .url in {<str>$url, <str>$alt_url};
                """,
                url=url,
                alt_url=alt_url,
            )

    async def test_http_auth_ext_local_password_register_form_02(self):
        with self.http_con() as http_con:
            provider_config = await self.get_builtin_provider_config_by_name(
                "local_emailpassword"
            )
            provider_name = provider_config.name
            email = f"{uuid.uuid4()}@example.com"

            # Different domain
            form_data = {
                "provider": provider_name,
                "email": email,
                "password": "test_password",
                "redirect_to": "https://not-on-the-allow-list.com/some/path",
                "challenge": str(uuid.uuid4()),
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

            # Non-matching port
            form_data["redirect_to"] = (
                "https://oauth.example.com:8080/app/some/path"
            )
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

            # Path doesn't match
            form_data["redirect_to"] = (
                "https://oauth.example.com/wrong-base/path"
            )
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

    async def test_http_auth_ext_local_password_register_form_no_smtp(self):
        await self.con.query(
            """
            CONFIGURE CURRENT DATABASE RESET
            current_email_provider_name;
            """,
        )
        await self._wait_for_db_config(
            "cfg::current_email_provider_name", is_reset=True
        )
        try:
            with self.http_con() as http_con:
                email = f"{uuid.uuid4()}@example.com"
                form_data = {
                    "provider": "builtin::local_emailpassword",
                    "email": email,
                    "password": "test_password",
                    "challenge": str(uuid.uuid4()),
                }
                form_data_encoded = urllib.parse.urlencode(form_data).encode()

                _, _, status = self.http_con_request(
                    http_con,
                    None,
                    path="register",
                    method="POST",
                    body=form_data_encoded,
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded"
                    },
                )

                self.assertEqual(status, 201)
        finally:
            await self.con.query(
                """
                CONFIGURE CURRENT DATABASE SET
                current_email_provider_name := "email_hosting_is_easy";
                """,
            )

    async def test_http_auth_ext_local_password_register_json_02(self):
        with self.http_con() as http_con:
            provider_name = "builtin::local_emailpassword"
            email = f"{uuid.uuid4()}@example.com"

            json_data = {
                "provider": provider_name,
                "email": email,
                "password": "test_password2",
                "challenge": str(uuid.uuid4()),
            }
            json_data_encoded = json.dumps(json_data).encode()

            body, _headers, status = self.http_con_request(
                http_con,
                None,
                path="register",
                method="POST",
                body=json_data_encoded,
                headers={"Content-Type": "application/json"},
            )

            self.assertEqual(status, 201)

            identity = await self.con.query_single(
                """
                with module ext::auth
                select assert_single((
                    select LocalIdentity
                    filter .<identity[is EmailPasswordFactor]
                        .email = <str>$email
                ));
                """,
                email=email,
            )

            pkce_challenge = await self.con.query_single(
                """
                SELECT ext::auth::PKCEChallenge { * }
                FILTER .challenge = <str>$challenge
                AND .identity.id = <uuid>$identity_id
                """,
                challenge=json_data["challenge"],
                identity_id=identity.id,
            )

            self.assertEqual(
                json.loads(body),
                {
                    "code": str(pkce_challenge.id),
                    "provider": "builtin::local_emailpassword",
                },
            )

            password_credential = await self.con.query(
                """
                SELECT ext::auth::EmailPasswordFactor { password_hash }
                FILTER .identity.id = <uuid>$identity
                """,
                identity=identity.id,
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
            email = f"{uuid.uuid4()}@example.com"
            form_data = {
                "email": email,
                "password": "test_password",
                "challenge": str(uuid.uuid4()),
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
            provider_config = await self.get_builtin_provider_config_by_name(
                "local_emailpassword"
            )
            provider_name = provider_config.name
            email = f"{uuid.uuid4()}@example.com"

            form_data = {
                "provider": provider_name,
                "email": email,
                "challenge": str(uuid.uuid4()),
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
            provider_config = await self.get_builtin_provider_config_by_name(
                "local_emailpassword"
            )
            provider_name = provider_config.name

            form_data = {
                "provider": provider_name,
                "password": "test_password",
                "challenge": str(uuid.uuid4()),
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
            provider_config = await self.get_builtin_provider_config_by_name(
                "local_emailpassword"
            )
            provider_name = provider_config.name
            email = f"{uuid.uuid4()}@example.com"

            # Register a new user
            form_data = {
                "provider": provider_name,
                "email": email,
                "password": "test_auth_password",
                "challenge": str(uuid.uuid4()),
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
                "challenge": str(uuid.uuid4()),
            }
            auth_data_encoded = urllib.parse.urlencode(auth_data).encode()

            body, _headers, status = self.http_con_request(
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
                        .email = <str>$email;
                """,
                email=email,
            )

            self.assertEqual(len(identity), 1)

            pkce_challenge = await self.con.query_single(
                """
                SELECT ext::auth::PKCEChallenge { * }
                FILTER .challenge = <str>$challenge
                AND .identity.id = <uuid>$identity_id
                """,
                challenge=auth_data["challenge"],
                identity_id=identity[0].id,
            )

            self.assertEqual(
                json.loads(body),
                {
                    "code": str(pkce_challenge.id),
                },
            )

            # Attempt to authenticate with wrong password
            auth_data_wrong_password = {
                "provider": form_data["provider"],
                "email": form_data["email"],
                "password": "wrong_password",
                "challenge": str(uuid.uuid4()),
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
            random_email = f"{uuid.uuid4()}@example.com"
            auth_data_random_handle = {
                "provider": form_data["provider"],
                "email": random_email,
                "password": form_data["password"],
                "challenge": str(uuid.uuid4()),
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
                "redirect_to": "https://example.com/app/some/path",
                "challenge": str(uuid.uuid4()),
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
                "redirect_to": "https://example.com/app/some/path",
                "redirect_on_failure": "https://example.com/app/failure/path",
                "challenge": str(uuid.uuid4()),
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

    async def test_http_auth_ext_local_emailpassword_resend_verification(self):
        with self.http_con() as http_con:
            # Register a new user
            provider_config = await self.get_builtin_provider_config_by_name(
                "local_emailpassword"
            )
            provider_name = provider_config.name
            email = f"{uuid.uuid4()}@example.com"
            form_data = {
                "provider": provider_name,
                "email": email,
                "password": "test_resend_password",
                "challenge": str(uuid.uuid4()),
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

            # Get the verification token from email
            file_name_hash = hashlib.sha256(
                f"{SENDER}{email}".encode()
            ).hexdigest()
            test_file = os.environ.get(
                "EDGEDB_TEST_EMAIL_FILE",
                f"/tmp/edb-test-email-{file_name_hash}.pickle",
            )
            with open(test_file, "rb") as f:
                email_args = pickle.load(f)
            self.assertEqual(email_args["sender"], SENDER)
            self.assertEqual(email_args["recipients"], form_data["email"])
            msg = cast(EmailMessage, email_args["message"]).get_body(
                ("html",)
            )
            assert msg is not None
            msg = cast(EmailMessage, email_args["message"]).get_body(
                ("html",)
            )
            assert msg is not None
            html_email = msg.get_payload(decode=True).decode("utf-8")
            match = re.search(
                r'<p style="word-break: break-all">([^<]+)',
                html_email,
            )
            assert match is not None
            verify_url = urllib.parse.urlparse(match.group(1))
            search_params = urllib.parse.parse_qs(verify_url.query)
            verification_token = search_params.get(
                "verification_token", [None]
            )[0]
            assert verification_token is not None

            # Resend verification email with the verification token
            resend_data = {
                "provider": form_data["provider"],
                "verification_token": verification_token,
            }
            resend_data_encoded = urllib.parse.urlencode(resend_data).encode()

            _, _, status = self.http_con_request(
                http_con,
                None,
                path="resend-verification-email",
                method="POST",
                body=resend_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(status, 200)

            # Resend verification email with just the email
            resend_data = {
                "provider": form_data["provider"],
                "email": email,
            }
            resend_data_encoded = urllib.parse.urlencode(resend_data).encode()

            _, _, status = self.http_con_request(
                http_con,
                None,
                path="resend-verification-email",
                method="POST",
                body=resend_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(status, 200)

            # Resend verification email with email and challenge
            resend_data = {
                "provider": form_data["provider"],
                "email": email,
                "challenge": form_data["challenge"],
            }
            resend_data_encoded = urllib.parse.urlencode(resend_data).encode()
            _, _, status = self.http_con_request(
                http_con,
                None,
                path="resend-verification-email",
                method="POST",
                body=resend_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            self.assertEqual(status, 200)

            # Resend verification email with email and code_challenge
            resend_data = {
                "provider": form_data["provider"],
                "email": email,
                "code_challenge": form_data["challenge"],
            }
            resend_data_encoded = urllib.parse.urlencode(resend_data).encode()
            _, _, status = self.http_con_request(
                http_con,
                None,
                path="resend-verification-email",
                method="POST",
                body=resend_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            self.assertEqual(status, 200)

            # Resend verification email with no email or token
            resend_data = {
                "provider": form_data["provider"],
            }
            resend_data_encoded = urllib.parse.urlencode(resend_data).encode()

            _, _, status = self.http_con_request(
                http_con,
                None,
                path="resend-verification-email",
                method="POST",
                body=resend_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(status, 400)

    async def test_http_auth_ext_local_webauthn_resend_verification(self):
        with self.http_con() as http_con:
            # Register a new user
            provider_config = await self.get_builtin_provider_config_by_name(
                "local_webauthn"
            )
            provider_name = provider_config.name
            email = f"{uuid.uuid4()}@example.com"
            credential_one = uuid.uuid4().bytes
            credential_two = uuid.uuid4().bytes

            await self.con.query_single(
                """
                with
                    email := <str>$email,
                    user_handle := <bytes>$user_handle,
                    credential_one := <bytes>$credential_one,
                    public_key_one := <bytes>$public_key_one,
                    credential_two := <bytes>$credential_two,
                    public_key_two := <bytes>$public_key_two,
                    factor_one := (insert ext::auth::WebAuthnFactor {
                        email := email,
                        user_handle := user_handle,
                        credential_id := credential_one,
                        public_key := public_key_one,
                        identity := (insert ext::auth::LocalIdentity {
                            issuer := "local",
                            subject := "",
                        }),
                    }),
                    factor_two := (insert ext::auth::WebAuthnFactor {
                        email := email,
                        user_handle := user_handle,
                        credential_id := credential_two,
                        public_key := public_key_two,
                        identity := (insert ext::auth::LocalIdentity {
                            issuer := "local",
                            subject := "",
                        }),
                    }),
                select true;
                """,
                email=email,
                user_handle=uuid.uuid4().bytes,
                credential_one=credential_one,
                public_key_one=uuid.uuid4().bytes,
                credential_two=credential_two,
                public_key_two=uuid.uuid4().bytes,
            )

            # Resend verification email with credential_id
            resend_data = {
                "provider": provider_name,
                "credential_id": base64.b64encode(credential_one).decode(),
            }
            resend_data_encoded = urllib.parse.urlencode(resend_data).encode()

            _, _, status = self.http_con_request(
                http_con,
                None,
                path="resend-verification-email",
                method="POST",
                body=resend_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(status, 200)

            file_name_hash = hashlib.sha256(
                f"{SENDER}{email}".encode()
            ).hexdigest()
            test_file = os.environ.get(
                "EDGEDB_TEST_EMAIL_FILE",
                f"/tmp/edb-test-email-{file_name_hash}.pickle",
            )
            with open(test_file, "rb") as f:
                email_args = pickle.load(f)
            self.assertEqual(email_args["sender"], SENDER)
            self.assertEqual(email_args["recipients"], email)
            msg = cast(EmailMessage, email_args["message"]).get_body(
                ("html",)
            )
            assert msg is not None
            html_email = msg.get_payload(decode=True).decode("utf-8")
            match = re.search(
                r'<p style="word-break: break-all">([^<]+)', html_email
            )
            assert match is not None
            verify_url = urllib.parse.urlparse(match.group(1))
            search_params = urllib.parse.parse_qs(verify_url.query)
            verification_token = search_params.get(
                "verification_token", [None]
            )[0]
            assert verification_token is not None

            # Resend verification email with the verification token
            resend_data = {
                "provider": provider_name,
                "verification_token": verification_token,
            }
            resend_data_encoded = urllib.parse.urlencode(resend_data).encode()

            _, _, status = self.http_con_request(
                http_con,
                None,
                path="resend-verification-email",
                method="POST",
                body=resend_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(status, 200)

            # Resend verification email with email
            resend_data = {
                "provider": provider_name,
                "email": email,
            }
            resend_data_encoded = urllib.parse.urlencode(resend_data).encode()

            _, _, status = self.http_con_request(
                http_con,
                None,
                path="resend-verification-email",
                method="POST",
                body=resend_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(status, 400)

    async def test_http_auth_ext_token_01(self):
        base_url = self.mock_net_server.get_base_url().rstrip("/")
        webhook_request = (
            "POST",
            base_url,
            "/webhook-02",
        )
        url = f"{webhook_request[1]}/{webhook_request[2]}"
        signing_secret_key = str(uuid.uuid4())
        await self.con.query(
            f"""
            CONFIGURE CURRENT DATABASE
            INSERT ext::auth::WebhookConfig {{
                url := <str>$url,
                events := {{
                    ext::auth::WebhookEvent.IdentityAuthenticated,
                }},
                signing_secret_key := <str>$signing_secret_key,
            }};
            """,
            url=url,
            signing_secret_key=signing_secret_key,
        )
        await self._wait_for_db_config("ext::auth::AuthConfig::webhooks")

        try:
            with self.http_con() as http_con:
                self.mock_net_server.register_route_handler(*webhook_request)(
                    (
                        "",
                        204,
                    )
                )

                # Create a PKCE challenge and verifier
                verifier = base64.urlsafe_b64encode(os.urandom(43)).rstrip(b'=')
                challenge = base64.urlsafe_b64encode(
                    hashlib.sha256(verifier).digest()
                ).rstrip(b'=')
                pkce = await self.con.query_single(
                    """
                    select (
                        insert ext::auth::PKCEChallenge {
                            challenge := <str>$challenge,
                            auth_token := <str>$auth_token,
                            refresh_token := <str>$refresh_token,
                            id_token := <str>$id_token,
                            identity := (
                                insert ext::auth::Identity {
                                    issuer := "https://example.com",
                                    subject := "abcdefg",
                                }
                            ),
                        }
                    ) {
                        id,
                        challenge,
                        auth_token,
                        refresh_token,
                        id_token,
                        identity_id := .identity.id
                    }
                    """,
                    challenge=challenge.decode(),
                    auth_token="a_provider_token",
                    refresh_token="a_refresh_token",
                    id_token="an_id_token",
                )

                # Correct code, random verifier
                (_, _, wrong_verifier_status) = self.http_con_request(
                    http_con,
                    {
                        "code": pkce.id,
                        "code_verifier": base64.urlsafe_b64encode(
                            os.urandom(43)
                        )
                        .rstrip(b"=")
                        .decode(),
                    },
                    path="token",
                )

                self.assertEqual(wrong_verifier_status, 403)

                # Correct code, correct verifier
                (
                    body,
                    _,
                    status,
                ) = self.http_con_request(
                    http_con,
                    {"code": pkce.id, "verifier": verifier.decode()},
                    path="token",
                )

                self.assertEqual(status, 200)
                body_json = json.loads(body)
                self.assertEqual(
                    body_json,
                    {
                        "auth_token": body_json["auth_token"],
                        "identity_id": str(pkce.identity_id),
                        "provider_token": "a_provider_token",
                        "provider_refresh_token": "a_refresh_token",
                        "provider_id_token": "an_id_token",
                    },
                )
                async for tr in self.try_until_succeeds(
                    delay=2, timeout=120, ignore=(KeyError, AssertionError)
                ):
                    async with tr:
                        requests_for_webhook = self.mock_net_server.requests[
                            webhook_request
                        ]
                        self.assertEqual(len(requests_for_webhook), 1)

                webhook_request = requests_for_webhook[0]
                maybe_json_body = webhook_request.body
                self.assertIsNotNone(maybe_json_body)
                assert maybe_json_body is not None
                event_data = json.loads(maybe_json_body)
                self.assertEqual(
                    event_data["event_type"],
                    "IdentityAuthenticated",
                )
                self.assertEqual(
                    event_data["identity_id"], str(pkce.identity_id)
                )
                signature = requests_for_webhook[0].headers[
                    "x-ext-auth-signature-sha256"
                ]

                self.assertEqual(
                    signature,
                    hmac.new(
                        signing_secret_key.encode(),
                        requests_for_webhook[0].body.encode(),
                        hashlib.sha256,
                    ).hexdigest(),
                )

                # Correct code, correct verifier, already used PKCE
                (_, _, replay_attack_status) = self.http_con_request(
                    http_con,
                    {"code": pkce.id, "verifier": verifier.decode()},
                    path="token",
                )

                self.assertEqual(replay_attack_status, 403)
        finally:
            await self.con.query(
                f"""
                CONFIGURE CURRENT DATABASE
                RESET ext::auth::WebhookConfig filter .url = <str>$url;
                """,
                url=url,
            )

    async def test_http_auth_ext_token_02(self):
        with self.http_con() as http_con:
            # Too short: 32-octet -> 43-octet base64url
            verifier = base64.urlsafe_b64encode(os.urandom(31)).rstrip(b'=')
            (_, _, status) = self.http_con_request(
                http_con,
                {
                    "code": str(uuid.uuid4()),
                    "verifier": verifier.decode(),
                },
                path="token",
            )

            self.assertEqual(status, 400)

    async def test_http_auth_ext_token_03(self):
        with self.http_con() as http_con:
            # Too long: 96-octet -> 128-octet base64url
            verifier = base64.urlsafe_b64encode(os.urandom(97)).rstrip(b'=')
            (_, _, status) = self.http_con_request(
                http_con,
                {
                    "code": str(uuid.uuid4()),
                    "verifier": verifier.decode(),
                },
                path="token",
            )

            self.assertEqual(status, 400)

    async def test_http_auth_ext_local_password_forgot_form_01(self):
        with self.http_con() as http_con:
            provider_name = "builtin::local_emailpassword"
            email = f"{uuid.uuid4()}@example.com"

            # Register a new user
            form_data = {
                "provider": provider_name,
                "email": email,
                "password": uuid.uuid4(),
                "challenge": uuid.uuid4(),
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

            # Send reset
            form_data = {
                "provider": provider_name,
                "reset_url": "https://example.com/app/reset-password",
                "email": email,
                "challenge": uuid.uuid4(),
            }
            form_data_encoded = urllib.parse.urlencode(form_data).encode()

            body, _, status = self.http_con_request(
                http_con,
                None,
                path="send-reset-email",
                method="POST",
                body=form_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(status, 200)

            identity = await self.con.query(
                """
                with module ext::auth
                SELECT LocalIdentity
                FILTER .<identity[is EmailPasswordFactor].email = <str>$email
                """,
                email=email,
            )
            self.assertEqual(len(identity), 1)

            data = json.loads(body)

            assert_data_shape.assert_data_shape(
                data,
                {
                    "email_sent": email,
                },
                self.fail,
            )

            file_name_hash = hashlib.sha256(
                f"{SENDER}{email}".encode()
            ).hexdigest()
            test_file = os.environ.get(
                "EDGEDB_TEST_EMAIL_FILE",
                f"/tmp/edb-test-email-{file_name_hash}.pickle",
            )
            with open(test_file, "rb") as f:
                email_args = pickle.load(f)
            self.assertEqual(email_args["sender"], SENDER)
            self.assertEqual(email_args["recipients"], email)
            msg = cast(EmailMessage, email_args["message"]).get_body(
                ("html",)
            )
            assert msg is not None
            html_email = msg.get_payload(decode=True).decode("utf-8")
            match = re.search(
                r'<p style="word-break: break-all">([^<]+)', html_email
            )
            assert match is not None
            reset_url = match.group(1)
            self.assertTrue(
                reset_url.startswith(form_data['reset_url'] + '?reset_token=')
            )
            claims = await self.extract_jwt_claims(
                reset_url.split('=', maxsplit=1)[1], "reset"
            )
            self.assertEqual(claims.get("sub"), str(identity[0].id))
            self.assertEqual(claims.get("iss"), str(self.http_addr))
            now = utcnow()
            tenMinutesLater = now + datetime.timedelta(minutes=10)
            self.assertTrue(claims.get("exp") > now.timestamp())
            self.assertTrue(claims.get("exp") < tenMinutesLater.timestamp())

            password_credential = await self.con.query(
                """
                SELECT ext::auth::EmailPasswordFactor { password_hash }
                FILTER .identity.id = <uuid>$identity
                """,
                identity=identity[0].id,
            )
            self.assertTrue(
                base64.b64encode(
                    hashlib.sha256(
                        password_credential[0].password_hash.encode()
                    ).digest()
                ).decode()
                == claims.get('jti')
            )

            # Send reset with redirect_to
            _, redirect_headers, redirect_status = self.http_con_request(
                http_con,
                None,
                path="send-reset-email",
                method="POST",
                body=urllib.parse.urlencode(
                    {
                        **form_data,
                        "redirect_to": "https://example.com/app/forgot-password",
                    }
                ).encode(),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(redirect_status, 302)
            location = redirect_headers.get("location")
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
                "https://example.com/app/forgot-password",
            )

            assert_data_shape.assert_data_shape(
                parsed_query,
                {
                    "email_sent": [email],
                },
                self.fail,
            )

            # Try sending reset for non existent user
            _, _, error_status = self.http_con_request(
                http_con,
                None,
                path="send-reset-email",
                method="POST",
                body=urllib.parse.urlencode(
                    {
                        **form_data,
                        "email": "invalid@example.com",
                    }
                ).encode(),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(error_status, 200)

    async def test_http_auth_ext_local_password_forgot_form_02(self):
        with self.http_con() as http_con:
            provider_name = "builtin::local_emailpassword"

            form_data = {
                "provider": provider_name,
                "reset_url": "https://not-on-the-allow-list.com/reset-password",
                "email": f"{uuid.uuid4()}@example.com",
                "challenge": uuid.uuid4(),
            }
            form_data_encoded = urllib.parse.urlencode(form_data).encode()

            _, _, status = self.http_con_request(
                http_con,
                None,
                path="send-reset-email",
                method="POST",
                body=form_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            self.assertEqual(status, 400)

    async def test_http_auth_ext_local_password_reset_form_01(self):
        with self.http_con() as http_con:
            provider_name = 'builtin::local_emailpassword'
            email = f"{uuid.uuid4()}@example.com"

            # Register a new user
            form_data = {
                "provider": provider_name,
                "email": email,
                "password": "test_auth_password",
                "challenge": uuid.uuid4(),
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
            email_password_factor = await self.con.query_single(
                """
                with module ext::auth
                select assert_single((
                    select EmailPasswordFactor { verified_at }
                    filter .email = <str>$email
                ))
                """,
                email=email,
            )
            self.assertIsNone(email_password_factor.verified_at)

            # Send reset
            verifier = base64.urlsafe_b64encode(os.urandom(32)).rstrip(b'=')
            challenge = (
                base64.urlsafe_b64encode(hashlib.sha256(verifier).digest())
                .rstrip(b'=')
                .decode()
            )
            form_data = {
                "provider": provider_name,
                "reset_url": "https://example.com/app/reset-password",
                "email": email,
                "challenge": challenge,
            }
            form_data_encoded = urllib.parse.urlencode(form_data).encode()
            body, _, status = self.http_con_request(
                http_con,
                None,
                path="send-reset-email",
                method="POST",
                body=form_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(status, 200)

            file_name_hash = hashlib.sha256(
                f"{SENDER}{email}".encode()
            ).hexdigest()
            test_file = os.environ.get(
                "EDGEDB_TEST_EMAIL_FILE",
                f"/tmp/edb-test-email-{file_name_hash}.pickle",
            )
            with open(test_file, "rb") as f:
                email_args = pickle.load(f)
            self.assertEqual(email_args["sender"], SENDER)
            self.assertEqual(email_args["recipients"], email)
            msg = cast(EmailMessage, email_args["message"]).get_body(
                ("html",)
            )
            assert msg is not None
            html_email = msg.get_payload(decode=True).decode("utf-8")
            match = re.search(
                r'<p style="word-break: break-all">([^<]+)', html_email
            )
            assert match is not None
            reset_url = match.group(1)
            self.assertTrue(
                reset_url.startswith(form_data['reset_url'] + '?reset_token=')
            )

            reset_token = reset_url.split('=', maxsplit=1)[1]

            # Update password
            auth_data = {
                "provider": provider_name,
                "reset_token": reset_token,
                "password": "new password",
            }
            auth_data_encoded = urllib.parse.urlencode(auth_data).encode()

            body, _, status = self.http_con_request(
                http_con,
                None,
                path="reset-password",
                method="POST",
                body=auth_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(status, 200)

            identity = await self.con.query(
                """
                with module ext::auth
                SELECT LocalIdentity
                FILTER .<identity[is EmailPasswordFactor].email
                        = <str>$email
                """,
                email=email,
            )

            self.assertEqual(len(identity), 1)

            email_password_factor = await self.con.query_single(
                """
                with module ext::auth
                SELECT EmailPasswordFactor { verified_at }
                FILTER .identity.id = <uuid>$identity_id
                """,
                identity_id=identity[0].id,
            )

            self.assertIsNotNone(email_password_factor.verified_at)

            pkce_challenge = await self.con.query_single(
                """
                with module ext::auth
                select PKCEChallenge { id }
                filter .identity.id = <uuid>$identity_id
                and .challenge = <str>$challenge
                """,
                identity_id=identity[0].id,
                challenge=challenge,
            )
            self.assertEqual(
                json.loads(body),
                {"code": str(pkce_challenge.id)},
            )

            # Try to re-use the reset token

            _, _, error_status = self.http_con_request(
                http_con,
                None,
                path="reset-password",
                method="POST",
                body=auth_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(error_status, 400)

            # Try to re-use the reset token (with redirect_on_failure)

            _, error_headers, error_status = self.http_con_request(
                http_con,
                None,
                path="reset-password",
                method="POST",
                body=urllib.parse.urlencode(
                    {
                        **auth_data,
                        "redirect_to": "https://example.com/app/",
                        "redirect_on_failure": (
                            "https://example.com/app/reset-password"
                        ),
                    }
                ).encode(),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(error_status, 302)
            location = error_headers.get("location")
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
                "https://example.com/app/reset-password",
            )

            self.assertEqual(
                parsed_query.get("error"),
                ["Invalid 'reset_token'"],
            )

    async def test_http_auth_ext_local_password_reset_form_02(self):
        with self.http_con() as http_con:
            provider_name = 'builtin::local_emailpassword'

            # Send reset
            verifier = base64.urlsafe_b64encode(os.urandom(32)).rstrip(b'=')
            challenge = (
                base64.urlsafe_b64encode(hashlib.sha256(verifier).digest())
                .rstrip(b'=')
                .decode()
            )
            form_data = {
                "provider": provider_name,
                "reset_url": "https://not-on-the-allow-list.com/reset-password",
                "email": f"{uuid.uuid4()}@example.com",
                "challenge": challenge,
            }
            form_data_encoded = urllib.parse.urlencode(form_data).encode()
            _, _, status = self.http_con_request(
                http_con,
                None,
                path="send-reset-email",
                method="POST",
                body=form_data_encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            self.assertEqual(status, 400)

    async def test_http_auth_ext_ui_signin(self):
        with self.http_con() as http_con:
            challenge = (
                base64.urlsafe_b64encode(os.urandom(32)).rstrip(b'=').decode()
            )
            query_params = urllib.parse.urlencode({"challenge": challenge})

            body, _, status = self.http_con_request(
                http_con,
                path=f"ui/signin?{query_params}",
            )

            body_str = body.decode()

            self.assertIn(f"{APP_NAME[:100]}...", body_str)
            self.assertIn(LOGO_URL, body_str)
            self.assertIn(BRAND_COLOR, body_str)

            # Check for OAuth buttons
            self.assertIn("Sign in with Google", body_str)
            self.assertIn("Sign in with GitHub", body_str)
            self.assertIn("Sign in with My Generic OIDC Provider", body_str)
            self.assertEqual(status, 200)

    async def test_http_auth_ext_webauthn_register_options(self):
        with self.http_con() as http_con:
            email = f"{uuid.uuid4()}@example.com"
            query_params = urllib.parse.urlencode({"email": email})

            body, headers, status = self.http_con_request(
                http_con,
                path=f"webauthn/register/options?{query_params}",
            )

            body_json = json.loads(body.decode())
            self.assertEqual(status, 200)

            # Check the structure of the PublicKeyCredentialCreationOptions
            self.assertIn("rp", body_json)
            self.assertIn("user", body_json)
            self.assertIn("challenge", body_json)
            self.assertIn("pubKeyCredParams", body_json)
            self.assertIn("timeout", body_json)
            self.assertIn("excludeCredentials", body_json)
            self.assertIn("attestation", body_json)

            self.assertIsInstance(body_json["rp"], dict)
            self.assertIn("name", body_json["rp"])
            self.assertEqual(body_json["rp"]["name"], f"{APP_NAME[:100]}...")
            self.assertIn("id", body_json["rp"])
            self.assertEqual(body_json["rp"]["id"], "example.com")

            self.assertIsInstance(body_json["user"], dict)
            self.assertIn("id", body_json["user"])
            self.assertIsInstance(body_json["user"]["id"], str)
            self.assertIn("name", body_json["user"])
            self.assertEqual(body_json["user"]["name"], email)
            self.assertIn("displayName", body_json["user"])
            self.assertEqual(body_json["user"]["displayName"], email)

            self.assertIsInstance(body_json["pubKeyCredParams"], list)
            self.assertTrue(len(body_json["pubKeyCredParams"]) > 0)
            for param in body_json["pubKeyCredParams"]:
                self.assertIn("type", param)
                self.assertEqual(param["type"], "public-key")
                self.assertIn("alg", param)
                self.assertIsInstance(param["alg"], int)

            self.assertEqual(body_json["timeout"], 60000)

            self.assertIsInstance(body_json["excludeCredentials"], list)

            self.assertEqual(body_json["attestation"], "none")

            challenge_bytes = base64.urlsafe_b64decode(
                f'{body_json["challenge"]}==='
            )
            user_handle = base64.urlsafe_b64decode(
                f'{body_json["user"]["id"]}==='
            )
            user_handle_cookie = self.maybe_get_cookie_value(
                headers, "edgedb-webauthn-registration-user-handle"
            )
            user_handle_cookie_value = base64.urlsafe_b64decode(
                f'{user_handle_cookie}==='
            )
            self.assertEqual(user_handle_cookie_value, user_handle)

            self.assertTrue(
                await self.con.query_single(
                    '''
                    SELECT EXISTS (
                        SELECT ext::auth::WebAuthnRegistrationChallenge
                        filter .challenge = <bytes>$challenge
                        AND .email = <str>$email
                        AND .user_handle = <bytes>$user_handle
                    )
                    ''',
                    challenge=challenge_bytes,
                    email=email,
                    user_handle=user_handle,
                )
            )

    async def test_http_auth_ext_webauthn_register_options_existing_user(self):
        email = f"{uuid.uuid4()}@example.com"
        existing_user_handle = uuid.uuid4().bytes

        # Insert two existing WebAuthnFactors for the email
        await self.con.query_single(
            """
            with
                email := <str>$email,
                user_handle := <bytes>$user_handle,
                credential_one := <bytes>$credential_one,
                public_key_one := <bytes>$public_key_one,
                credential_two := <bytes>$credential_two,
                public_key_two := <bytes>$public_key_two,
                factor_one := (insert ext::auth::WebAuthnFactor {
                    email := email,
                    user_handle := user_handle,
                    credential_id := credential_one,
                    public_key := public_key_one,
                    identity := (insert ext::auth::LocalIdentity {
                        issuer := "local",
                        subject := "",
                    }),
                }),
                factor_two := (insert ext::auth::WebAuthnFactor {
                    email := email,
                    user_handle := user_handle,
                    credential_id := credential_two,
                    public_key := public_key_two,
                    identity := (insert ext::auth::LocalIdentity {
                        issuer := "local",
                        subject := "",
                    }),
                }),
            select true;
            """,
            email=email,
            user_handle=existing_user_handle,
            credential_one=uuid.uuid4().bytes,
            public_key_one=uuid.uuid4().bytes,
            credential_two=uuid.uuid4().bytes,
            public_key_two=uuid.uuid4().bytes,
        )

        with self.http_con() as http_con:
            body, _, status = self.http_con_request(
                http_con,
                path=f"webauthn/register/options?email={email}",
            )

            self.assertEqual(status, 200)

            body_json = json.loads(body)
            self.assertIn("user", body_json)
            self.assertIn("id", body_json["user"])
            user_id_decoded = base64.urlsafe_b64decode(
                f'{body_json["user"]["id"]}==='
            )

            self.assertEqual(user_id_decoded, existing_user_handle)

    async def test_http_auth_ext_webauthn_emails_share_user_handle(self):
        email = f"{uuid.uuid4()}@example.com"

        user_handle_one = uuid.uuid4().bytes
        credential_id_one = uuid.uuid4().bytes
        public_key_one = uuid.uuid4().bytes

        user_handle_two = uuid.uuid4().bytes
        credential_id_two = uuid.uuid4().bytes
        public_key_two = uuid.uuid4().bytes

        with self.assertRaisesRegex(
            QueryAssertionError,
            "user_handle must be the same for a given email",
        ):
            await self.con.execute(
                """
                with
                    factor_one := (insert ext::auth::WebAuthnFactor {
                        email := <str>$email,
                        user_handle := <bytes>$user_handle_one,
                        credential_id := <bytes>$credential_id_one,
                        public_key := <bytes>$public_key_one,
                        identity := (insert ext::auth::LocalIdentity {
                            issuer := "local",
                            subject := "",
                        }),
                    }),
                    factor_two := (insert ext::auth::WebAuthnFactor {
                        email := <str>$email,
                        user_handle := <bytes>$user_handle_two,
                        credential_id := <bytes>$credential_id_two,
                        public_key := <bytes>$public_key_two,
                        identity := (insert ext::auth::LocalIdentity {
                            issuer := "local",
                            subject := "",
                        }),
                    })
                select true;
                """,
                email=email,
                user_handle_one=user_handle_one,
                credential_id_one=credential_id_one,
                public_key_one=public_key_one,
                user_handle_two=user_handle_two,
                credential_id_two=credential_id_two,
                public_key_two=public_key_two,
            )

    async def test_http_auth_ext_webauthn_authenticate_options(self):
        with self.http_con() as http_con:
            email = f"{uuid.uuid4()}@example.com"
            user_handle = uuid.uuid4().bytes
            credential_id = uuid.uuid4().bytes
            public_key = uuid.uuid4().bytes

            await self.con.query_single(
                """
                with identity := (insert ext::auth::LocalIdentity {
                    issuer := "local",
                    subject := "",
                })
                INSERT ext::auth::WebAuthnFactor {
                    email := <str>$email,
                    user_handle := <bytes>$user_handle,
                    credential_id := <bytes>$credential_id,
                    public_key := <bytes>$public_key,
                    identity := identity,
                };
                """,
                email=email,
                user_handle=user_handle,
                credential_id=credential_id,
                public_key=public_key,
            )

            body, _headers, status = self.http_con_request(
                http_con,
                path=f"webauthn/authenticate/options?email={email}",
            )

            self.assertEqual(status, 200)

            body_json = json.loads(body)
            self.assertIn("challenge", body_json)
            self.assertIsInstance(body_json["challenge"], str)
            self.assertIn("rpId", body_json)
            self.assertIsInstance(body_json["rpId"], str)
            self.assertIn("timeout", body_json)
            self.assertIsInstance(body_json["timeout"], int)
            self.assertIn("allowCredentials", body_json)
            self.assertIsInstance(body_json["allowCredentials"], list)
            allow_credentials = body_json["allowCredentials"]
            self.assertTrue(
                all(
                    "type" in cred and "id" in cred
                    for cred in allow_credentials
                ),
                "Each credential should have 'type' and 'id' keys",
            )
            self.assertIn(
                base64.urlsafe_b64encode(credential_id).rstrip(b"=").decode(),
                [cred["id"] for cred in allow_credentials],
                (
                    "The generated credential_id should be in the "
                    "'allowCredentials' list"
                ),
            )

            challenge_bytes = base64.urlsafe_b64decode(
                f'{body_json["challenge"]}==='
            )
            self.assertTrue(
                await self.con.query_single(
                    '''
                    SELECT EXISTS (
                        SELECT ext::auth::WebAuthnAuthenticationChallenge
                        filter .challenge = <bytes>$challenge
                        AND any(
                            .factors.email = <str>$email
                            AND .factors.user_handle = <bytes>$user_handle
                        )
                    )
                    ''',
                    challenge=challenge_bytes,
                    email=email,
                    user_handle=user_handle,
                )
            )

    async def test_http_auth_ext_magic_link_with_link_url(self):
        email = f"{uuid.uuid4()}@example.com"
        challenge = "test_challenge"
        callback_url = "https://example.com/app/auth/callback"
        redirect_on_failure = "https://example.com/app/auth/magic-link-failure"
        link_url = "https://example.com/app/magic-link/authenticate"

        with self.http_con() as http_con:
            # Register with link_url
            _, _, status = self.http_con_request(
                http_con,
                method="POST",
                path="magic-link/register",
                body=json.dumps(
                    {
                        "provider": "builtin::local_magic_link",
                        "email": email,
                        "challenge": challenge,
                        "callback_url": callback_url,
                        "redirect_on_failure": redirect_on_failure,
                        "link_url": link_url,
                    }
                ).encode(),
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
            self.assertEqual(status, 200)

            # Get the token from email
            file_name_hash = hashlib.sha256(
                f"{SENDER}{email}".encode()
            ).hexdigest()
            test_file = os.environ.get(
                "EDGEDB_TEST_EMAIL_FILE",
                f"/tmp/edb-test-email-{file_name_hash}.pickle",
            )
            with open(test_file, "rb") as f:
                email_args = pickle.load(f)
            self.assertEqual(email_args["sender"], SENDER)
            self.assertEqual(email_args["recipients"], email)
            msg = cast(EmailMessage, email_args["message"]).get_body(
                ("html",)
            )
            assert msg is not None
            html_email = msg.get_payload(decode=True).decode("utf-8")
            match = re.search(
                r'<p style="word-break: break-all">([^<]+)', html_email
            )
            assert match is not None
            magic_link_url = urllib.parse.urlparse(match.group(1))
            search_params = urllib.parse.parse_qs(magic_link_url.query)
            token = search_params.get("token", [None])[0]
            assert token is not None
            self.assertEqual(
                urllib.parse.urlunparse(
                    (
                        magic_link_url.scheme,
                        magic_link_url.netloc,
                        magic_link_url.path,
                        '',
                        '',
                        '',
                    )
                ),
                link_url,
            )

            _, headers, status = self.http_con_request(
                http_con,
                method="GET",
                path=f"magic-link/authenticate?token={token}",
            )

            self.assertEqual(status, 302)
            location = headers.get("location")
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
                callback_url,
            )

            # Sign in with the registered email and link_url
            _, _, status = self.http_con_request(
                http_con,
                method="POST",
                path="magic-link/email",
                body=json.dumps(
                    {
                        "provider": "builtin::local_magic_link",
                        "email": email,
                        "challenge": challenge,
                        "callback_url": callback_url,
                        "redirect_on_failure": redirect_on_failure,
                        "link_url": link_url,
                    }
                ).encode(),
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )

            # Get the token from email
            file_name_hash = hashlib.sha256(
                f"{SENDER}{email}".encode()
            ).hexdigest()
            test_file = os.environ.get(
                "EDGEDB_TEST_EMAIL_FILE",
                f"/tmp/edb-test-email-{file_name_hash}.pickle",
            )
            with open(test_file, "rb") as f:
                email_args = pickle.load(f)
            self.assertEqual(email_args["sender"], SENDER)
            self.assertEqual(email_args["recipients"], email)
            msg = cast(EmailMessage, email_args["message"]).get_body(
                ("html",)
            )
            assert msg is not None
            html_email = msg.get_payload(decode=True).decode("utf-8")
            match = re.search(
                r'<p style="word-break: break-all">([^<]+)', html_email
            )
            assert match is not None
            magic_link_url = urllib.parse.urlparse(match.group(1))
            search_params = urllib.parse.parse_qs(magic_link_url.query)
            token = search_params.get("token", [None])[0]
            assert token is not None
            self.assertEqual(
                urllib.parse.urlunparse(
                    (
                        magic_link_url.scheme,
                        magic_link_url.netloc,
                        magic_link_url.path,
                        '',
                        '',
                        '',
                    )
                ),
                link_url,
            )

    async def test_http_auth_ext_magic_link_without_link_url(self):
        email = f"{uuid.uuid4()}@example.com"
        challenge = "test_challenge"
        callback_url = "https://example.com/app/auth/callback"
        redirect_on_failure = "https://example.com/app/auth/magic-link-failure"

        with self.http_con() as http_con:
            # Register without link_url
            _, _, status = self.http_con_request(
                http_con,
                method="POST",
                path="magic-link/register",
                body=json.dumps(
                    {
                        "provider": "builtin::local_magic_link",
                        "email": email,
                        "challenge": challenge,
                        "callback_url": callback_url,
                        "redirect_on_failure": redirect_on_failure,
                    }
                ).encode(),
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
            self.assertEqual(status, 200)

            # Get the token from email
            file_name_hash = hashlib.sha256(
                f"{SENDER}{email}".encode()
            ).hexdigest()
            test_file = os.environ.get(
                "EDGEDB_TEST_EMAIL_FILE",
                f"/tmp/edb-test-email-{file_name_hash}.pickle",
            )
            with open(test_file, "rb") as f:
                email_args = pickle.load(f)
            self.assertEqual(email_args["sender"], SENDER)
            self.assertEqual(email_args["recipients"], email)
            msg = cast(EmailMessage, email_args["message"]).get_body(
                ("html",)
            )
            assert msg is not None
            html_email = msg.get_payload(decode=True).decode("utf-8")
            match = re.search(
                r'<p style="word-break: break-all">([^<]+)', html_email
            )
            assert match is not None
            magic_link_url = urllib.parse.urlparse(match.group(1))
            search_params = urllib.parse.parse_qs(magic_link_url.query)
            token = search_params.get("token", [None])[0]
            assert token is not None
            self.assertEqual(
                urllib.parse.urlunparse(
                    (
                        magic_link_url.scheme,
                        magic_link_url.netloc,
                        magic_link_url.path,
                        '',
                        '',
                        '',
                    )
                ),
                f"{self.http_addr}/magic-link/authenticate",
            )

            _, headers, status = self.http_con_request(
                http_con,
                method="GET",
                path=f"magic-link/authenticate?token={token}",
            )

            self.assertEqual(status, 302)
            location = headers.get("location")
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
                callback_url,
            )

            # Sign in with the registered email without link_url
            _, _, status = self.http_con_request(
                http_con,
                method="POST",
                path="magic-link/email",
                body=json.dumps(
                    {
                        "provider": "builtin::local_magic_link",
                        "email": email,
                        "challenge": challenge,
                        "callback_url": callback_url,
                        "redirect_on_failure": redirect_on_failure,
                    }
                ).encode(),
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )

            # Get the token from email
            file_name_hash = hashlib.sha256(
                f"{SENDER}{email}".encode()
            ).hexdigest()
            test_file = os.environ.get(
                "EDGEDB_TEST_EMAIL_FILE",
                f"/tmp/edb-test-email-{file_name_hash}.pickle",
            )
            with open(test_file, "rb") as f:
                email_args = pickle.load(f)
            self.assertEqual(email_args["sender"], SENDER)
            self.assertEqual(email_args["recipients"], email)
            msg = cast(EmailMessage, email_args["message"]).get_body(
                ("html",)
            )
            assert msg is not None
            html_email = msg.get_payload(decode=True).decode("utf-8")
            match = re.search(
                r'<p style="word-break: break-all">([^<]+)', html_email
            )
            assert match is not None
            magic_link_url = urllib.parse.urlparse(match.group(1))
            search_params = urllib.parse.parse_qs(magic_link_url.query)
            token = search_params.get("token", [None])[0]
            assert token is not None
            self.assertEqual(
                urllib.parse.urlunparse(
                    (
                        magic_link_url.scheme,
                        magic_link_url.netloc,
                        magic_link_url.path,
                        '',
                        '',
                        '',
                    )
                ),
                f"{self.http_addr}/magic-link/authenticate",
            )

    async def test_http_auth_ext_identity_delete_cascade_01(self):
        """
        Test deleting a LocalIdentity deletes the associated Factors and
        PKCEChallenge objects as well
        """
        result = await self.con.query_single(
            """
            with
                identity := (insert ext::auth::LocalIdentity {
                    issuer := "local",
                    subject := "",
                }),
                factor := (insert ext::auth::EmailPasswordFactor {
                    identity := identity,
                    email := "test@example.com",
                    password_hash := "abc123",
                }),
                pkce_challenge := (insert ext::auth::PKCEChallenge {
                    identity := identity,
                    challenge := "abc123",
                }),
            select identity;
            """,
        )

        await self.con.query(
            "delete <ext::auth::Identity><uuid>$identity_id;",
            identity_id=result.id,
        )

    async def test_http_auth_ext_identity_delete_cascade_02(self):
        """
        Test deleting an Identity deletes the associated objects as well
        """

        result = await self.con.query_single(
            """
            with
                identity := (insert ext::auth::Identity {
                    issuer := "https://example.com",
                    subject := "abc123",
                }),
                pkce_challenge := (insert ext::auth::PKCEChallenge {
                    identity := identity,
                    challenge := "123abc",
                }),
            select identity;
            """,
        )

        await self.con.query(
            "delete <ext::auth::Identity><uuid>$identity_id;",
            identity_id=result.id,
        )

    async def test_http_auth_ext_identity_delete_cascade_03(self):
        """
        Test deleting a WebAuthn LocalIdentity deletes the associated
        WebAuthnFactor and WebAuthnRegistrationChallenge
        """

        challenge = uuid.uuid4().bytes
        user_handle = uuid.uuid4().bytes
        credential_id = uuid.uuid4().bytes
        public_key = uuid.uuid4().bytes

        result = await self.con.query_single(
            """
            with
                user_handle := <bytes>$user_handle,
                credential_id := <bytes>$credential_id,
                public_key := <bytes>$public_key,
                challenge := <bytes>$challenge,
                identity := (insert ext::auth::LocalIdentity {
                    issuer := "local",
                    subject := "",
                }),
                factor := (insert ext::auth::WebAuthnFactor {
                    identity := identity,
                    user_handle := user_handle,
                    email := "test@example.com",
                    credential_id := credential_id,
                    public_key := public_key,
                }),
                challenge := (insert ext::auth::WebAuthnRegistrationChallenge {
                    challenge := challenge,
                    email := "test@example.com",
                    user_handle := user_handle,
                }),
                pkce_challenge := (insert ext::auth::PKCEChallenge {
                    identity := identity,
                    challenge := "abc123",
                }),
            select identity;
            """,
            user_handle=user_handle,
            credential_id=credential_id,
            public_key=public_key,
            challenge=challenge,
        )

        await self.con.query(
            "delete <ext::auth::LocalIdentity><uuid>$identity_id;",
            identity_id=result.id,
        )

    async def test_client_token_identity_card(self):
        await self.con.query_single(
            '''
            select global ext::auth::ClientTokenIdentity
        '''
        )

    async def test_http_auth_ext_static_files(self):
        with self.http_con() as http_con:
            _, _, status = self.http_con_request(
                http_con,
                path="ui/_static/icon_github.svg",
            )

            self.assertEqual(status, 200)

    async def test_edgeql_introspection_secret(self):
        await self.assert_query_result(
            '''
            SELECT schema::Property { name }
            FILTER .secret AND .source.name = 'ext::auth::AuthConfig';
            ''',
            [{'name': 'auth_signing_key'}],
        )
