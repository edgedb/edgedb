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


from __future__ import annotations

import datetime
import http
import http.cookies
import json
import logging
import urllib.parse
import base64
import hashlib
import os
import mimetypes
import uuid
import dataclasses

from typing import (
    Any,
    Optional,
    Tuple,
    FrozenSet,
    cast,
    TYPE_CHECKING,
    Callable,
)

import aiosmtplib
from jwcrypto import jwk, jwt

from edb import errors as edb_errors
from edb.common import debug
from edb.common import markup
from edb.ir import statypes
from edb.server import tenant as edbtenant, metrics
from edb.server.config.types import CompositeConfigType

from . import (
    email_password,
    oauth,
    errors,
    util,
    pkce,
    ui,
    config,
    email as auth_emails,
    webauthn,
    magic_link,
    webhook,
)
from .data import EmailFactor

if TYPE_CHECKING:
    from edb.server.protocol import protocol


logger = logging.getLogger('edb.server.ext.auth')


class Router:
    test_url: Optional[str]

    def __init__(
        self,
        *,
        db: edbtenant.dbview.Database,
        base_path: str,
        tenant: edbtenant.Tenant,
    ):
        self.db = db
        self.base_path = base_path
        self.tenant = tenant
        self.test_mode = tenant.server.in_test_mode()

    def _get_url_munger(
        self, request: protocol.HttpRequest
    ) -> Callable[[str], str] | None:
        """
        Returns a callable that can be used to modify the base URL
        when making requests to the OAuth provider.

        This is used to redirect requests to the test OAuth provider
        when running in test mode.
        """
        if not self.test_mode:
            return None
        test_url = (
            request.params[b'oauth-test-server'].decode()
            if (request.params and b'oauth-test-server' in request.params)
            else None
        )
        if test_url:
            return lambda path: f"{test_url}{urllib.parse.quote(path)}"
        return None

    async def handle_request(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
        args: list[str],
    ) -> None:
        if self.db.db_config is None:
            await self.db.introspection()

        self.test_url = (
            request.params[b'oauth-test-server'].decode()
            if (
                self.test_mode
                and request.params
                and b'oauth-test-server' in request.params
            )
            else None
        )

        logger.info(
            f"Handling incoming HTTP request: /ext/auth/{'/'.join(args)}"
        )

        try:
            match args:
                # PKCE token exchange route
                case ("token",):
                    await self.handle_token(request, response)

                # OAuth routes
                case ("authorize",):
                    await self.handle_authorize(request, response)
                case ("callback",):
                    await self.handle_callback(request, response)

                # Email/password routes
                case ("register",):
                    await self.handle_register(request, response)
                case ("authenticate",):
                    await self.handle_authenticate(request, response)
                case ('send-reset-email',):
                    await self.handle_send_reset_email(request, response)
                case ('reset-password',):
                    await self.handle_reset_password(request, response)

                # Magic link routes
                case ('magic-link', 'register'):
                    await self.handle_magic_link_register(request, response)
                case ('magic-link', 'email'):
                    await self.handle_magic_link_email(request, response)
                case ('magic-link', 'authenticate'):
                    await self.handle_magic_link_authenticate(request, response)

                # WebAuthn routes
                case ('webauthn', 'register'):
                    await self.handle_webauthn_register(request, response)
                case ('webauthn', 'register', 'options'):
                    await self.handle_webauthn_register_options(
                        request, response
                    )
                case ('webauthn', 'authenticate'):
                    await self.handle_webauthn_authenticate(request, response)
                case ('webauthn', 'authenticate', 'options'):
                    await self.handle_webauthn_authenticate_options(
                        request, response
                    )

                # Email verification routes
                case ("verify",):
                    await self.handle_verify(request, response)
                case ("resend-verification-email",):
                    await self.handle_resend_verification_email(
                        request, response
                    )

                # UI routes
                case ('ui', 'signin'):
                    await self.handle_ui_signin(request, response)
                case ('ui', 'signup'):
                    await self.handle_ui_signup(request, response)
                case ('ui', 'forgot-password'):
                    await self.handle_ui_forgot_password(request, response)
                case ('ui', 'reset-password'):
                    await self.handle_ui_reset_password(request, response)
                case ("ui", "verify"):
                    await self.handle_ui_verify(request, response)
                case ("ui", "resend-verification"):
                    await self.handle_ui_resend_verification(request, response)
                case ("ui", "magic-link-sent"):
                    await self.handle_ui_magic_link_sent(request, response)
                case ('ui', '_static', filename):
                    filepath = os.path.join(
                        os.path.dirname(__file__), '_static', filename
                    )
                    try:
                        with open(filepath, 'rb') as f:
                            response.status = http.HTTPStatus.OK
                            response.content_type = (
                                mimetypes.guess_type(filename)[0]
                                or 'application/octet-stream'
                            ).encode()
                            response.body = f.read()
                    except FileNotFoundError:
                        response.status = http.HTTPStatus.NOT_FOUND

                case _:
                    raise errors.NotFound("Unknown auth endpoint")

        # User-facing errors
        except errors.NotFound as ex:
            _fail_with_error(
                response=response,
                status=http.HTTPStatus.NOT_FOUND,
                ex=ex,
            )

        except errors.InvalidData as ex:
            _fail_with_error(
                response=response,
                status=http.HTTPStatus.BAD_REQUEST,
                ex=ex,
            )

        except errors.PKCEVerificationFailed as ex:
            _fail_with_error(
                response=response,
                status=http.HTTPStatus.FORBIDDEN,
                ex=ex,
            )

        except errors.NoIdentityFound as ex:
            _fail_with_error(
                response=response,
                status=http.HTTPStatus.FORBIDDEN,
                ex=ex,
            )

        except errors.UserAlreadyRegistered as ex:
            _fail_with_error(
                response=response,
                status=http.HTTPStatus.CONFLICT,
                ex=ex,
            )

        except errors.VerificationRequired as ex:
            _fail_with_error(
                response=response,
                status=http.HTTPStatus.UNAUTHORIZED,
                ex=ex,
            )

        # Server errors
        except errors.MissingConfiguration as ex:
            _fail_with_error(
                response=response,
                status=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                ex=ex,
            )

        except errors.WebAuthnAuthenticationFailed as ex:
            _fail_with_error(
                response=response,
                status=http.HTTPStatus.UNAUTHORIZED,
                ex=ex,
            )

        except Exception as ex:
            if debug.flags.server:
                markup.dump(ex)
            _fail_with_error(
                response=response,
                status=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                ex=edb_errors.InternalServerError(str(ex)),
            )

    async def handle_authorize(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        query = urllib.parse.parse_qs(
            request.url.query.decode("ascii") if request.url.query else ""
        )
        provider_name = _get_search_param(query, "provider")
        allowed_redirect_to = self._make_allowed_url(
            _get_search_param(query, "redirect_to")
        )
        allowed_redirect_to_on_signup = self._maybe_make_allowed_url(
            _maybe_get_search_param(query, "redirect_to_on_signup")
        )
        allowed_callback_url = self._maybe_make_allowed_url(
            _maybe_get_search_param(query, "callback_url")
        )
        challenge = _get_search_param(
            query, "challenge", fallback_keys=["code_challenge"]
        )
        oauth_client = oauth.Client(
            db=self.db,
            provider_name=provider_name,
            url_munger=self._get_url_munger(request),
            http_client=self.tenant.get_http_client(originator="auth"),
        )
        await pkce.create(self.db, challenge)
        authorize_url = await oauth_client.get_authorize_url(
            redirect_uri=(
                allowed_callback_url.url
                if allowed_callback_url
                else self._get_callback_url()
            ),
            state=self._make_state_claims(
                provider_name,
                allowed_redirect_to.url,
                (
                    allowed_redirect_to_on_signup.url
                    if allowed_redirect_to_on_signup
                    else None
                ),
                challenge,
            ),
        )
        # n.b. Explicitly allow authorization URL to be outside of allowed
        # URLs because it is a trusted URL from the identity provider.
        self._do_redirect(response, AllowedUrl(authorize_url))

    async def handle_callback(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        if request.method == b"POST" and (
            request.content_type == b"application/x-www-form-urlencoded"
        ):
            form_data = urllib.parse.parse_qs(request.body.decode())
            state = _maybe_get_form_field(form_data, "state")
            code = _maybe_get_form_field(form_data, "code")

            error = _maybe_get_form_field(form_data, "error")
            error_description = _maybe_get_form_field(
                form_data, "error_description"
            )
        elif request.url.query is not None:
            query = urllib.parse.parse_qs(
                request.url.query.decode("ascii") if request.url.query else ""
            )
            state = _maybe_get_search_param(query, "state")
            code = _maybe_get_search_param(query, "code")
            error = _maybe_get_search_param(query, "error")
            error_description = _maybe_get_search_param(
                query, "error_description"
            )
        else:
            raise errors.OAuthProviderFailure(
                "Provider did not respond with expected data"
            )

        if state is None:
            raise errors.InvalidData(
                "Provider did not include the 'state' parameter in " "callback"
            )

        if error is not None:
            try:
                claims = self._verify_and_extract_claims(state)
                redirect_to = cast(str, claims["redirect_to"])
            except Exception:
                raise errors.InvalidData("Invalid state token")

            params = {
                "error": error,
            }
            error_str = error
            if error_description is not None:
                params["error_description"] = error_description
                error_str += f": {error_description}"

            logger.debug(f"OAuth provider returned an error: {error_str}")
            return self._try_redirect(
                response,
                util.join_url_params(redirect_to, params),
            )

        if code is None:
            raise errors.InvalidData(
                "Provider did not include the 'code' parameter in " "callback"
            )

        try:
            claims = self._verify_and_extract_claims(state)
            provider_name = cast(str, claims["provider"])
            allowed_redirect_to = self._make_allowed_url(
                cast(str, claims["redirect_to"])
            )
            allowed_redirect_to_on_signup = self._maybe_make_allowed_url(
                cast(Optional[str], claims.get("redirect_to_on_signup"))
            )
            challenge = cast(str, claims["challenge"])
        except Exception:
            raise errors.InvalidData("Invalid state token")
        oauth_client = oauth.Client(
            db=self.db,
            provider_name=provider_name,
            url_munger=self._get_url_munger(request),
            http_client=self.tenant.get_http_client(originator="auth"),
        )
        (
            identity,
            new_identity,
            auth_token,
            refresh_token,
            id_token,
        ) = await oauth_client.handle_callback(code, self._get_callback_url())
        pkce_code = await pkce.link_identity_challenge(
            self.db, identity.id, challenge
        )
        if auth_token or refresh_token:
            await pkce.add_provider_tokens(
                self.db,
                id=pkce_code,
                auth_token=auth_token,
                refresh_token=refresh_token,
                id_token=id_token,
            )
        new_url = (
            (allowed_redirect_to_on_signup or allowed_redirect_to)
            if new_identity
            else allowed_redirect_to
        ).map(
            lambda u: util.join_url_params(
                u, {"code": pkce_code, "provider": provider_name}
            )
        )
        logger.info(
            "OAuth callback successful: "
            f"identity_id={identity.id}, new_identity={new_identity}"
        )
        self._do_redirect(response, new_url)

    async def handle_token(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        query = urllib.parse.parse_qs(
            request.url.query.decode("ascii") if request.url.query else ""
        )
        code = _get_search_param(query, "code")
        verifier = _get_search_param(
            query, "verifier", fallback_keys=["code_verifier"]
        )

        verifier_size = len(verifier)

        if verifier_size < 43:
            raise errors.InvalidData(
                "Verifier must be at least 43 characters long"
            )
        if verifier_size > 128:
            raise errors.InvalidData(
                "Verifier must be shorter than 128 " "characters long"
            )
        try:
            pkce_object = await pkce.get_by_id(self.db, code)
        except Exception:
            raise errors.NoIdentityFound("Could not find a matching PKCE code")

        if pkce_object.identity_id is None:
            raise errors.InvalidData("Code is not associated with an Identity")

        hashed_verifier = hashlib.sha256(verifier.encode()).digest()
        base64_url_encoded_verifier = base64.urlsafe_b64encode(
            hashed_verifier
        ).rstrip(b'=')

        if base64_url_encoded_verifier.decode() == pkce_object.challenge:
            await pkce.delete(self.db, code)

            identity_id = pkce_object.identity_id
            await self._maybe_send_webhook(
                webhook.IdentityAuthenticated(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.datetime.now(datetime.timezone.utc),
                    identity_id=identity_id,
                )
            )
            session_token = self._make_session_token(identity_id)
            logger.info(f"Token exchange successful: identity_id={identity_id}")
            response.status = http.HTTPStatus.OK
            response.content_type = b"application/json"
            response.body = json.dumps(
                {
                    "auth_token": session_token,
                    "identity_id": identity_id,
                    "provider_token": pkce_object.auth_token,
                    "provider_refresh_token": pkce_object.refresh_token,
                    "provider_id_token": pkce_object.id_token,
                }
            ).encode()
        else:
            raise errors.PKCEVerificationFailed

    async def handle_register(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        data = self._get_data_from_request(request)

        allowed_redirect_to = self._maybe_make_allowed_url(
            cast(Optional[str], data.get("redirect_to"))
        )

        maybe_challenge = cast(Optional[str], data.get("challenge"))
        register_provider_name = cast(Optional[str], data.get("provider"))
        if register_provider_name is None:
            raise errors.InvalidData('Missing "provider" in register request')

        email_password_client = email_password.Client(db=self.db)
        require_verification = email_password_client.config.require_verification
        if not require_verification and maybe_challenge is None:
            raise errors.InvalidData(
                'Missing "challenge" in register request'
            )
        pkce_code: Optional[str] = None

        try:
            email_factor = await email_password_client.register(data)
            identity = email_factor.identity

            verify_url = data.get("verify_url", f"{self.base_path}/ui/verify")
            verification_token = self._make_verification_token(
                identity_id=identity.id,
                verify_url=verify_url,
                maybe_challenge=maybe_challenge,
                maybe_redirect_to=(
                    allowed_redirect_to.url if allowed_redirect_to else None
                ),
            )

            await self._maybe_send_webhook(
                webhook.IdentityCreated(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.datetime.now(datetime.timezone.utc),
                    identity_id=identity.id,
                )
            )
            await self._maybe_send_webhook(
                webhook.EmailFactorCreated(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.datetime.now(datetime.timezone.utc),
                    identity_id=identity.id,
                    email_factor_id=email_factor.id,
                )
            )
            await self._maybe_send_webhook(
                webhook.EmailVerificationRequested(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.datetime.now(datetime.timezone.utc),
                    identity_id=identity.id,
                    email_factor_id=email_factor.id,
                    verification_token=verification_token,
                )
            )

            if require_verification:
                response_dict = {
                    "identity_id": identity.id,
                    "verification_email_sent_at": datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat()
                }
            else:
                # Checked at the beginning of the route handler
                assert maybe_challenge is not None
                await pkce.create(self.db, maybe_challenge)
                pkce_code = await pkce.link_identity_challenge(
                    self.db, identity.id, maybe_challenge
                )
                response_dict = {
                    "code": pkce_code,
                    "provider": register_provider_name,
                }

            await self._send_verification_email(
                provider=register_provider_name,
                verification_token=verification_token,
                to_addr=data["email"],
                verify_url=verify_url,
            )

            logger.info(
                f"Identity created: identity_id={identity.id}, "
                f"pkce_id={pkce_code!r}"
            )

            if allowed_redirect_to is not None:
                self._do_redirect(
                    response,
                    allowed_redirect_to.map(
                        lambda u: util.join_url_params(u, response_dict)
                    ),
                )
            else:
                response.status = http.HTTPStatus.CREATED
                response.content_type = b"application/json"
                response.body = json.dumps(response_dict).encode()
        except Exception as ex:
            redirect_on_failure = data.get(
                "redirect_on_failure", data.get("redirect_to")
            )
            if redirect_on_failure is not None:
                error_message = str(ex)
                email = data.get("email", "")
                logger.error(
                    f"Error creating identity: error={error_message}, "
                    f"email={email}"
                )
                error_redirect_url = util.join_url_params(
                    redirect_on_failure,
                    {
                        "error": error_message,
                        "email": email,
                    },
                )
                return self._try_redirect(response, error_redirect_url)
            else:
                raise ex

    async def handle_authenticate(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        data = self._get_data_from_request(request)

        _check_keyset(data, {"provider", "challenge", "email", "password"})
        challenge = data["challenge"]
        email = data["email"]
        password = data["password"]

        await pkce.create(self.db, challenge)

        allowed_redirect_to = self._maybe_make_allowed_url(
            cast(Optional[str], data.get("redirect_to"))
        )

        email_password_client = email_password.Client(db=self.db)
        try:
            local_identity = await email_password_client.authenticate(
                email, password
            )
            verified_at = (
                await email_password_client.get_verified_by_identity_id(
                    identity_id=local_identity.id
                )
            )
            if (
                email_password_client.config.require_verification
                and verified_at is None
            ):
                raise errors.VerificationRequired()

            pkce_code = await pkce.link_identity_challenge(
                self.db, local_identity.id, challenge
            )
            response_dict = {"code": pkce_code}
            logger.info(
                f"Authentication successful: identity_id={local_identity.id}, "
                f"pkce_id={pkce_code}"
            )
            if allowed_redirect_to:
                self._do_redirect(
                    response,
                    allowed_redirect_to.map(
                        lambda u: util.join_url_params(u, response_dict)
                    ),
                )
            else:
                response.status = http.HTTPStatus.OK
                response.content_type = b"application/json"
                response.body = json.dumps(response_dict).encode()
        except Exception as ex:
            redirect_on_failure = data.get(
                "redirect_on_failure", data.get("redirect_to")
            )
            if redirect_on_failure is not None:
                error_message = str(ex)
                email = data.get("email", "")
                logger.error(
                    f"Error authenticating: error={error_message}, "
                    f"email={email}"
                )
                error_redirect_url = util.join_url_params(
                    redirect_on_failure,
                    {
                        "error": error_message,
                        "email": email,
                    },
                )
                return self._try_redirect(response, error_redirect_url)
            else:
                raise ex

    async def handle_verify(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        data = self._get_data_from_request(request)

        _check_keyset(data, {"verification_token", "provider"})

        (
            identity_id,
            issued_at,
            _,
            maybe_challenge,
            maybe_redirect_to,
        ) = self._get_data_from_verification_token(data["verification_token"])

        try:
            email_factor = await self._try_verify_email(
                provider=data["provider"],
                issued_at=issued_at,
                identity_id=identity_id,
            )
            await self._maybe_send_webhook(
                webhook.EmailVerified(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.datetime.now(datetime.timezone.utc),
                    identity_id=identity_id,
                    email_factor_id=email_factor.id,
                )
            )
        except errors.VerificationTokenExpired:
            response.status = http.HTTPStatus.FORBIDDEN
            response.content_type = b"application/json"
            error_message = (
                "The 'iat' claim in verification token is older than 24 hours"
            )
            logger.error(f"Verification token expired: {error_message}")
            response.body = json.dumps({"message": error_message}).encode()
            return

        logger.info(
            f"Email verified: identity_id={identity_id}, "
            f"email_factor_id={email_factor.id}, "
            f"email={email_factor.email}"
        )
        match (maybe_challenge, maybe_redirect_to):
            case (str(challenge), str(redirect_to)):
                await pkce.create(self.db, challenge)
                code = await pkce.link_identity_challenge(
                    self.db, identity_id, challenge
                )
                return self._try_redirect(
                    response,
                    util.join_url_params(redirect_to, {"code": code}),
                )
            case (str(challenge), _):
                await pkce.create(self.db, challenge)
                code = await pkce.link_identity_challenge(
                    self.db, identity_id, challenge
                )
                response.status = http.HTTPStatus.OK
                response.content_type = b"application/json"
                response.body = json.dumps({"code": code}).encode()
                return
            case (_, str(redirect_to)):
                return self._try_redirect(response, redirect_to)
            case (_, _):
                response.status = http.HTTPStatus.NO_CONTENT
                return

    async def handle_resend_verification_email(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        request_data = self._get_data_from_request(request)

        _check_keyset(request_data, {"provider"})
        provider_name = request_data["provider"]
        local_client: email_password.Client | webauthn.Client
        match provider_name:
            case "builtin::local_emailpassword":
                local_client = email_password.Client(db=self.db)
            case "builtin::local_webauthn":
                local_client = webauthn.Client(db=self.db)
            case _:
                raise errors.InvalidData(
                    f"Unsupported provider: {request_data['provider']}"
                )

        verify_url = request_data.get(
            "verify_url", f"{self.base_path}/ui/verify"
        )
        email_factor: Optional[EmailFactor] = None
        if "verification_token" in request_data:
            (
                identity_id,
                _,
                verify_url,
                maybe_challenge,
                maybe_redirect_to,
            ) = self._get_data_from_verification_token(
                request_data["verification_token"]
            )
            email_factor = await local_client.get_email_factor_by_identity_id(
                identity_id
            )
        else:
            maybe_challenge = request_data.get(
                "challenge", request_data.get("code_challenge")
            )
            maybe_redirect_to = request_data.get("redirect_to")
            if maybe_redirect_to and not self._is_url_allowed(
                maybe_redirect_to
            ):
                raise errors.InvalidData(
                    "Redirect URL does not match any allowed URLs.",
                )
            match local_client:
                case webauthn.Client():
                    _check_keyset(request_data, {"credential_id"})
                    credential_id = base64.b64decode(
                        request_data["credential_id"]
                    )
                    email_factor = (
                        await local_client.get_email_factor_by_credential_id(
                            credential_id
                        )
                    )
                case email_password.Client():
                    _check_keyset(request_data, {"email"})
                    email_factor = await local_client.get_email_factor_by_email(
                        request_data["email"]
                    )

        if email_factor is None:
            match local_client:
                case webauthn.Client():
                    logger.debug(
                        f"Failed to find email factor for resend verification "
                        f"email: provider={provider_name}, "
                        f"webauthn_credential_id={request_data.get('credential_id')}"
                    )
                case email_password.Client():
                    logger.debug(
                        f"Failed to find email factor for resend verification "
                        f"email: provider={provider_name}, "
                        f"email={request_data.get('email')}"
                    )
            await auth_emails.send_fake_email(self.tenant)
        else:
            logger.info(
                f"Resending verification email: provider={provider_name}, "
                f"identity_id={email_factor.identity.id}, "
                f"email_factor_id={email_factor.id}, "
                f"email={email_factor.email}"
            )
            verification_token = self._make_verification_token(
                identity_id=email_factor.identity.id,
                verify_url=verify_url,
                maybe_challenge=maybe_challenge,
                maybe_redirect_to=maybe_redirect_to,
            )
            await self._maybe_send_webhook(
                webhook.EmailVerificationRequested(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.datetime.now(datetime.timezone.utc),
                    identity_id=email_factor.identity.id,
                    email_factor_id=email_factor.id,
                    verification_token=verification_token,
                )
            )
            await self._send_verification_email(
                provider=request_data["provider"],
                verification_token=verification_token,
                to_addr=email_factor.email,
                verify_url=verify_url,
            )

        response.status = http.HTTPStatus.OK

    async def handle_send_reset_email(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        data = self._get_data_from_request(request)

        _check_keyset(data, {"provider", "email", "reset_url", "challenge"})
        email = data["email"]
        email_password_client = email_password.Client(db=self.db)
        if not self._is_url_allowed(data["reset_url"]):
            raise errors.InvalidData(
                "Redirect URL does not match any allowed URLs.",
            )
        allowed_redirect_to = self._maybe_make_allowed_url(
            data.get("redirect_to")
        )

        try:
            try:
                (
                    email_factor,
                    secret,
                ) = await email_password_client.get_email_factor_and_secret(
                    email
                )
                identity_id = email_factor.identity.id

                new_reset_token = self._make_secret_token(
                    identity_id,
                    secret,
                    "reset",
                    {"challenge": data["challenge"]},
                )

                reset_token_params = {"reset_token": new_reset_token}
                reset_url = util.join_url_params(
                    data['reset_url'], reset_token_params
                )
                await self._maybe_send_webhook(
                    webhook.PasswordResetRequested(
                        event_id=str(uuid.uuid4()),
                        timestamp=datetime.datetime.now(datetime.timezone.utc),
                        identity_id=identity_id,
                        reset_token=new_reset_token,
                        email_factor_id=email_factor.id,
                    )
                )

                await auth_emails.send_password_reset_email(
                    db=self.db,
                    tenant=self.tenant,
                    to_addr=email,
                    reset_url=reset_url,
                    test_mode=self.test_mode,
                )
            except errors.NoIdentityFound:
                logger.debug(
                    f"Failed to find identity for send reset email: "
                    f"email={email}"
                )
                await auth_emails.send_fake_email(self.tenant)

            return_data = {
                "email_sent": email,
            }

            if allowed_redirect_to:
                return self._do_redirect(
                    response,
                    allowed_redirect_to.map(
                        lambda u: util.join_url_params(u, return_data)
                    ),
                )
            else:
                response.status = http.HTTPStatus.OK
                response.content_type = b"application/json"
                response.body = json.dumps(return_data).encode()
        except aiosmtplib.SMTPException as ex:
            if not debug.flags.server:
                logger.warning("Failed to send emails via SMTP", exc_info=True)
            raise edb_errors.InternalServerError(
                "Failed to send the email, please try again later."
            ) from ex

        except Exception as ex:
            redirect_on_failure = data.get(
                "redirect_on_failure", data.get("redirect_to")
            )
            if redirect_on_failure is not None:
                error_message = str(ex)
                logger.error(
                    f"Error sending reset email: error={error_message}, "
                    f"email={email}"
                )
                redirect_url = util.join_url_params(
                    redirect_on_failure,
                    {
                        "error": error_message,
                        "email": email,
                    },
                )
                return self._try_redirect(
                    response,
                    redirect_url,
                )
            else:
                raise ex

    async def handle_reset_password(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        data = self._get_data_from_request(request)

        _check_keyset(data, {"provider", "reset_token", "password"})
        reset_token = data['reset_token']
        password = data['password']
        email_password_client = email_password.Client(db=self.db)

        allowed_redirect_to = self._maybe_make_allowed_url(
            data.get("redirect_to")
        )

        try:

            identity_id, secret, challenge = self._get_data_from_reset_token(
                reset_token
            )

            await email_password_client.update_password(
                identity_id, secret, password
            )
            await pkce.create(self.db, challenge)
            code = await pkce.link_identity_challenge(
                self.db, identity_id, challenge
            )
            response_dict = {"code": code}
            logger.info(
                f"Reset password: identity_id={identity_id}, pkce_id={code}"
            )

            if allowed_redirect_to:
                return self._do_redirect(
                    response,
                    allowed_redirect_to.map(
                        lambda u: util.join_url_params(u, response_dict)
                    ),
                )
            else:
                response.status = http.HTTPStatus.OK
                response.content_type = b"application/json"
                response.body = json.dumps(response_dict).encode()
        except Exception as ex:
            redirect_on_failure = data.get(
                "redirect_on_failure", data.get("redirect_to")
            )
            if redirect_on_failure is not None:
                error_message = str(ex)
                logger.error(
                    f"Error resetting password: error={error_message}, "
                    f"reset_token={reset_token}"
                )
                redirect_url = util.join_url_params(
                    redirect_on_failure,
                    {
                        "error": error_message,
                        "reset_token": reset_token,
                    },
                )
                return self._try_redirect(response, redirect_url)
            else:
                raise ex

    async def handle_magic_link_register(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        data = self._get_data_from_request(request)

        _check_keyset(
            data,
            {
                "provider",
                "email",
                "challenge",
                "callback_url",
                "redirect_on_failure",
            },
        )

        email = data["email"]
        challenge = data["challenge"]
        callback_url = data["callback_url"]
        if not self._is_url_allowed(callback_url):
            raise errors.InvalidData(
                "Callback URL does not match any allowed URLs.",
            )

        allowed_redirect_on_failure = self._make_allowed_url(
            data["redirect_on_failure"]
        )

        allowed_redirect_to = self._maybe_make_allowed_url(
            data.get("redirect_to")
        )

        allowed_link_url = self._maybe_make_allowed_url(data.get("link_url"))
        link_url = (
            allowed_link_url.url
            if allowed_link_url
            else f"{self.base_path}/magic-link/authenticate"
        )

        magic_link_client = magic_link.Client(
            db=self.db,
            issuer=self.base_path,
            tenant=self.tenant,
            test_mode=self.test_mode,
        )

        request_accepts_json: bool = request.accept == b"application/json"

        if not request_accepts_json and not allowed_redirect_to:
            raise errors.InvalidData(
                "Request must accept JSON or provide a redirect URL."
            )

        try:
            email_factor = await magic_link_client.register(
                email=email,
            )
            await self._maybe_send_webhook(
                webhook.IdentityCreated(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.datetime.now(datetime.timezone.utc),
                    identity_id=email_factor.identity.id,
                )
            )
            await self._maybe_send_webhook(
                webhook.EmailFactorCreated(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.datetime.now(datetime.timezone.utc),
                    identity_id=email_factor.identity.id,
                    email_factor_id=email_factor.id,
                )
            )
            magic_link_token = magic_link_client.make_magic_link_token(
                identity_id=email_factor.identity.id,
                callback_url=callback_url,
                challenge=challenge,
            )
            await self._maybe_send_webhook(
                webhook.MagicLinkRequested(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.datetime.now(datetime.timezone.utc),
                    identity_id=email_factor.identity.id,
                    email_factor_id=email_factor.id,
                    magic_link_token=magic_link_token,
                    magic_link_url=link_url,
                )
            )
            logger.info(
                f"Sending magic link: identity_id={email_factor.identity.id}, "
                f"email={email}"
            )
            await magic_link_client.send_magic_link(
                email=email,
                link_url=link_url,
                redirect_on_failure=allowed_redirect_on_failure.url,
                token=magic_link_token,
            )

            return_data = {
                "email_sent": email,
            }

            if request_accepts_json:
                response.status = http.HTTPStatus.OK
                response.content_type = b"application/json"
                response.body = json.dumps(return_data).encode()
            elif allowed_redirect_to:
                return self._do_redirect(
                    response,
                    allowed_redirect_to.map(
                        lambda u: util.join_url_params(u, return_data)
                    ),
                )
            else:
                # This should not happen since we check earlier for this case
                # but this seems safer than a cast
                raise errors.InvalidData(
                    "Request must accept JSON or provide a redirect URL."
                )
        except Exception as ex:
            if request_accepts_json:
                raise ex

            error_message = str(ex)
            logger.error(
                f"Error sending magic link: error={error_message}, "
                f"email={email}"
            )
            redirect_url = allowed_redirect_on_failure.map(
                lambda u: util.join_url_params(
                    u,
                    {
                        "error": error_message,
                        "email": email,
                    },
                )
            )
            return self._do_redirect(response, redirect_url)

    async def handle_magic_link_email(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        data = self._get_data_from_request(request)

        try:
            _check_keyset(
                data,
                {
                    "provider",
                    "email",
                    "challenge",
                    "callback_url",
                    "redirect_on_failure",
                },
            )

            email = data["email"]
            challenge = data["challenge"]
            callback_url = data["callback_url"]
            redirect_on_failure = data["redirect_on_failure"]
            if not self._is_url_allowed(callback_url):
                raise errors.InvalidData(
                    "Callback URL does not match any allowed URLs.",
                )
            if not self._is_url_allowed(redirect_on_failure):
                raise errors.InvalidData(
                    "Error redirect URL does not match any allowed URLs.",
                )

            allowed_redirect_to = self._maybe_make_allowed_url(
                data.get("redirect_to")
            )

            allowed_link_url = self._maybe_make_allowed_url(
                data.get("link_url")
            )
            link_url = (
                allowed_link_url.url
                if allowed_link_url
                else f"{self.base_path}/magic-link/authenticate"
            )

            magic_link_client = magic_link.Client(
                db=self.db,
                issuer=self.base_path,
                tenant=self.tenant,
                test_mode=self.test_mode,
            )
            email_factor = await magic_link_client.get_email_factor_by_email(
                email
            )
            if email_factor is None:
                logger.error(
                    f"Cannot send magic link email: no email factor found for "
                    f"email={email}"
                )
                await auth_emails.send_fake_email(self.tenant)
            else:
                identity_id = email_factor.identity.id
                magic_link_token = magic_link_client.make_magic_link_token(
                    identity_id=identity_id,
                    callback_url=callback_url,
                    challenge=challenge,
                )
                await self._maybe_send_webhook(
                    webhook.MagicLinkRequested(
                        event_id=str(uuid.uuid4()),
                        timestamp=datetime.datetime.now(datetime.timezone.utc),
                        identity_id=identity_id,
                        email_factor_id=email_factor.id,
                        magic_link_token=magic_link_token,
                        magic_link_url=link_url,
                    )
                )
                await magic_link_client.send_magic_link(
                    email=email,
                    token=magic_link_token,
                    link_url=link_url,
                    redirect_on_failure=redirect_on_failure,
                )
                logger.info(
                    "Sent magic link email: "
                    f"identity_id={identity_id}, email={email}"
                )

            return_data = {
                "email_sent": email,
            }

            if allowed_redirect_to:
                return self._do_redirect(
                    response,
                    allowed_redirect_to.map(
                        lambda u: util.join_url_params(u, return_data)
                    ),
                )
            else:
                response.status = http.HTTPStatus.OK
                response.content_type = b"application/json"
                response.body = json.dumps(return_data).encode()
        except Exception as ex:
            redirect_on_failure = data.get(
                "redirect_on_failure", data.get("redirect_to")
            )
            if redirect_on_failure is None:
                raise ex
            else:
                error_message = str(ex)
                logger.error(
                    f"Error sending magic link email: error={error_message}, "
                    f"email={email}"
                )
                error_redirect_url = util.join_url_params(
                    redirect_on_failure,
                    {
                        "error": error_message,
                        "email": email,
                    },
                )
                self._try_redirect(response, error_redirect_url)

    async def handle_magic_link_authenticate(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        query = urllib.parse.parse_qs(
            request.url.query.decode("ascii") if request.url.query else ""
        )
        token = _get_search_param(query, "token")

        try:
            (identity_id, challenge, callback_url) = (
                self._get_data_from_magic_link_token(token)
            )
            await pkce.create(self.db, challenge)
            code = await pkce.link_identity_challenge(
                self.db, identity_id, challenge
            )
            local_client = magic_link.Client(
                db=self.db,
                tenant=self.tenant,
                test_mode=self.test_mode,
                issuer=self.base_path,
            )
            await local_client.verify_email(
                identity_id, datetime.datetime.now(datetime.timezone.utc)
            )

            return self._try_redirect(
                response,
                util.join_url_params(callback_url, {"code": code}),
            )

        except Exception as ex:
            redirect_on_failure = _maybe_get_search_param(
                query, "redirect_on_failure"
            )
            if redirect_on_failure is None:
                raise ex
            else:
                error_message = str(ex)
                logger.error(
                    f"Error authenticating magic link: error={error_message}, "
                    f"token={token}"
                )
                redirect_url = util.join_url_params(
                    redirect_on_failure,
                    {
                        "error": error_message,
                    },
                )
                return self._try_redirect(response, redirect_url)

    async def handle_webauthn_register_options(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        query = urllib.parse.parse_qs(
            request.url.query.decode("ascii") if request.url.query else ""
        )
        email = _get_search_param(query, "email")
        webauthn_client = webauthn.Client(self.db)

        (user_handle, registration_options) = (
            await webauthn_client.create_registration_options_for_email(
                email=email,
            )
        )

        response.status = http.HTTPStatus.OK
        response.content_type = b"application/json"
        _set_cookie(
            response,
            "edgedb-webauthn-registration-user-handle",
            user_handle,
            path="/",
        )
        response.body = registration_options

    async def handle_webauthn_register(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        data = self._get_data_from_request(request)

        _check_keyset(
            data,
            {"provider", "challenge", "email", "credentials", "verify_url"},
        )
        webauthn_client = webauthn.Client(self.db)

        provider_name: str = data["provider"]
        email: str = data["email"]
        verify_url: str = data["verify_url"]
        credentials: str = data["credentials"]
        pkce_challenge: str = data["challenge"]

        user_handle_cookie = request.cookies.get(
            "edgedb-webauthn-registration-user-handle"
        )
        user_handle_base64url: Optional[str] = (
            user_handle_cookie.value
            if user_handle_cookie
            else data.get("user_handle")
        )
        if user_handle_base64url is None:
            raise errors.InvalidData(
                "Missing user_handle from cookie or request body"
            )
        try:
            user_handle = base64.urlsafe_b64decode(
                f"{user_handle_base64url}==="
            )
        except Exception as e:
            raise errors.InvalidData("Failed to decode user_handle") from e

        require_verification = webauthn_client.provider.require_verification
        pkce_code: Optional[str] = None

        email_factor = await webauthn_client.register(
            credentials=credentials,
            email=email,
            user_handle=user_handle,
        )
        identity_id = email_factor.identity.id

        await self._maybe_send_webhook(
            webhook.IdentityCreated(
                event_id=str(uuid.uuid4()),
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                identity_id=identity_id,
            )
        )
        await self._maybe_send_webhook(
            webhook.EmailFactorCreated(
                event_id=str(uuid.uuid4()),
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                identity_id=identity_id,
                email_factor_id=email_factor.id,
            )
        )

        verification_token = self._make_verification_token(
            identity_id=identity_id,
            verify_url=verify_url,
            maybe_challenge=pkce_challenge,
            maybe_redirect_to=None,
        )

        await self._maybe_send_webhook(
            webhook.EmailVerificationRequested(
                event_id=str(uuid.uuid4()),
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                identity_id=identity_id,
                email_factor_id=email_factor.id,
                verification_token=verification_token,
            )
        )
        await self._send_verification_email(
            provider=provider_name,
            verification_token=verification_token,
            to_addr=email_factor.email,
            verify_url=verify_url,
        )

        if not require_verification:
            await pkce.create(self.db, pkce_challenge)
            pkce_code = await pkce.link_identity_challenge(
                self.db, identity_id, pkce_challenge
            )

        _set_cookie(
            response,
            "edgedb-webauthn-registration-user-handle",
            "",
            path="/",
        )
        response.status = http.HTTPStatus.CREATED
        response.content_type = b"application/json"
        if require_verification:
            now_iso8601 = datetime.datetime.now(
                datetime.timezone.utc
            ).isoformat()
            response.body = json.dumps(
                {
                    "identity_id": identity_id,
                    "verification_email_sent_at": now_iso8601,
                }
            ).encode()
            logger.info(
                f"Sent verification email: identity_id={identity_id}, "
                f"email={email}"
            )
        else:
            if pkce_code is None:
                raise errors.PKCECreationFailed
            response.body = json.dumps(
                {"code": pkce_code, "provider": provider_name}
            ).encode()
            logger.info(
                f"WebAuthn registration successful: identity_id={identity_id}, "
                f"email={email}, "
                f"pkce_id={pkce_code}"
            )

    async def handle_webauthn_authenticate_options(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        query = urllib.parse.parse_qs(
            request.url.query.decode("ascii") if request.url.query else ""
        )
        email = _get_search_param(query, "email")
        webauthn_provider = self._get_webauthn_provider()
        if webauthn_provider is None:
            raise errors.MissingConfiguration(
                "ext::auth::AuthConfig::providers",
                "WebAuthn provider is not configured",
            )
        webauthn_client = webauthn.Client(self.db)

        (_, registration_options) = (
            await webauthn_client.create_authentication_options_for_email(
                email=email, webauthn_provider=webauthn_provider
            )
        )

        response.status = http.HTTPStatus.OK
        response.content_type = b"application/json"
        response.body = registration_options

    async def handle_webauthn_authenticate(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        data = self._get_data_from_request(request)

        _check_keyset(
            data,
            {"challenge", "email", "assertion"},
        )
        webauthn_client = webauthn.Client(self.db)

        email: str = data["email"]
        assertion: str = data["assertion"]
        pkce_challenge: str = data["challenge"]

        identity = await webauthn_client.authenticate(
            assertion=assertion,
            email=email,
        )

        require_verification = webauthn_client.provider.require_verification
        if require_verification:
            email_is_verified = await webauthn_client.is_email_verified(
                email, assertion
            )
            if not email_is_verified:
                raise errors.VerificationRequired()

        await pkce.create(self.db, pkce_challenge)
        code = await pkce.link_identity_challenge(
            self.db, identity.id, pkce_challenge
        )

        logger.info(
            f"WebAuthn authentication successful: identity_id={identity.id}, "
            f"email={email}, "
            f"pkce_id={code}"
        )

        response.status = http.HTTPStatus.OK
        response.content_type = b"application/json"
        response.body = json.dumps(
            {
                "code": code,
            }
        ).encode()

    async def handle_ui_signin(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        ui_config = self._get_ui_config()

        if ui_config is None:
            response.status = http.HTTPStatus.NOT_FOUND
            response.body = b'Auth UI not enabled'
        else:
            providers = util.maybe_get_config(
                self.db,
                "ext::auth::AuthConfig::providers",
                frozenset,
            )
            if providers is None or len(providers) == 0:
                raise errors.MissingConfiguration(
                    'ext::auth::AuthConfig::providers',
                    'No providers are configured',
                )

            app_details_config = self._get_app_details_config()
            query = urllib.parse.parse_qs(
                request.url.query.decode("ascii") if request.url.query else ""
            )

            maybe_challenge = _get_pkce_challenge(
                response=response,
                cookies=request.cookies,
                query_dict=query,
            )
            if maybe_challenge is None:
                raise errors.InvalidData(
                    'Missing "challenge" in register request'
                )

            response.status = http.HTTPStatus.OK
            response.content_type = b'text/html'
            response.body = ui.render_signin_page(
                base_path=self.base_path,
                providers=providers,
                redirect_to=ui_config.redirect_to,
                redirect_to_on_signup=ui_config.redirect_to_on_signup,
                error_message=_maybe_get_search_param(query, 'error'),
                email=_maybe_get_search_param(query, 'email'),
                challenge=maybe_challenge,
                selected_tab=_maybe_get_search_param(query, 'selected_tab'),
                app_name=app_details_config.app_name,
                logo_url=app_details_config.logo_url,
                dark_logo_url=app_details_config.dark_logo_url,
                brand_color=app_details_config.brand_color,
            )

    async def handle_ui_signup(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        ui_config = self._get_ui_config()
        if ui_config is None:
            response.status = http.HTTPStatus.NOT_FOUND
            response.body = b'Auth UI not enabled'
        else:
            providers = util.maybe_get_config(
                self.db,
                "ext::auth::AuthConfig::providers",
                frozenset,
            )
            if providers is None or len(providers) == 0:
                raise errors.MissingConfiguration(
                    'ext::auth::AuthConfig::providers',
                    'No providers are configured',
                )

            query = urllib.parse.parse_qs(
                request.url.query.decode("ascii") if request.url.query else ""
            )

            maybe_challenge = _get_pkce_challenge(
                response=response,
                cookies=request.cookies,
                query_dict=query,
            )
            if maybe_challenge is None:
                raise errors.InvalidData(
                    'Missing "challenge" in register request'
                )
            app_details_config = self._get_app_details_config()

            response.status = http.HTTPStatus.OK
            response.content_type = b'text/html'
            response.body = ui.render_signup_page(
                base_path=self.base_path,
                providers=providers,
                redirect_to=ui_config.redirect_to,
                redirect_to_on_signup=ui_config.redirect_to_on_signup,
                error_message=_maybe_get_search_param(query, 'error'),
                email=_maybe_get_search_param(query, 'email'),
                challenge=maybe_challenge,
                selected_tab=_maybe_get_search_param(query, 'selected_tab'),
                app_name=app_details_config.app_name,
                logo_url=app_details_config.logo_url,
                dark_logo_url=app_details_config.dark_logo_url,
                brand_color=app_details_config.brand_color,
            )

    async def handle_ui_forgot_password(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        ui_config = self._get_ui_config()
        password_provider = (
            self._get_password_provider() if ui_config is not None else None
        )

        if ui_config is None or password_provider is None:
            response.status = http.HTTPStatus.NOT_FOUND
            response.body = (
                b'Password provider not configured'
                if ui_config
                else b'Auth UI not enabled'
            )
        else:
            query = urllib.parse.parse_qs(
                request.url.query.decode("ascii") if request.url.query else ""
            )
            challenge = _get_search_param(
                query, "challenge", fallback_keys=["code_challenge"]
            )
            app_details_config = self._get_app_details_config()

            response.status = http.HTTPStatus.OK
            response.content_type = b'text/html'
            response.body = ui.render_forgot_password_page(
                base_path=self.base_path,
                provider_name=password_provider.name,
                error_message=_maybe_get_search_param(query, 'error'),
                email=_maybe_get_search_param(query, 'email'),
                email_sent=_maybe_get_search_param(query, 'email_sent'),
                challenge=challenge,
                app_name=app_details_config.app_name,
                logo_url=app_details_config.logo_url,
                dark_logo_url=app_details_config.dark_logo_url,
                brand_color=app_details_config.brand_color,
            )

    async def handle_ui_reset_password(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        ui_config = self._get_ui_config()
        password_provider = (
            self._get_password_provider() if ui_config is not None else None
        )
        challenge: Optional[str] = None

        if ui_config is None or password_provider is None:
            response.status = http.HTTPStatus.NOT_FOUND
            response.body = (
                b'Password provider not configured'
                if ui_config
                else b'Auth UI not enabled'
            )
        else:
            query = urllib.parse.parse_qs(
                request.url.query.decode("ascii") if request.url.query else ""
            )

            reset_token = _maybe_get_search_param(query, 'reset_token')

            if reset_token is not None:
                try:
                    (
                        identity_id,
                        secret,
                        challenge,
                    ) = self._get_data_from_reset_token(reset_token)

                    email_password_client = email_password.Client(
                        db=self.db,
                    )

                    is_valid = (
                        await email_password_client.validate_reset_secret(
                            identity_id, secret
                        )
                        is not None
                    )
                except Exception:
                    is_valid = False
            else:
                is_valid = False

            app_details_config = self._get_app_details_config()
            response.status = http.HTTPStatus.OK
            response.content_type = b'text/html'
            response.body = ui.render_reset_password_page(
                base_path=self.base_path,
                provider_name=password_provider.name,
                is_valid=is_valid,
                redirect_to=ui_config.redirect_to,
                reset_token=reset_token,
                challenge=challenge,
                error_message=_maybe_get_search_param(query, 'error'),
                app_name=app_details_config.app_name,
                logo_url=app_details_config.logo_url,
                dark_logo_url=app_details_config.dark_logo_url,
                brand_color=app_details_config.brand_color,
            )

    async def handle_ui_verify(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        error_messages: list[str] = []
        ui_config = self._get_ui_config()
        if ui_config is None:
            response.status = http.HTTPStatus.NOT_FOUND
            response.body = b'Auth UI not enabled'
            return

        is_valid = True
        maybe_pkce_code: str | None = None
        redirect_to = ui_config.redirect_to_on_signup or ui_config.redirect_to

        query = urllib.parse.parse_qs(
            request.url.query.decode("ascii") if request.url.query else ""
        )

        maybe_provider_name = _maybe_get_search_param(query, "provider")
        maybe_verification_token = _maybe_get_search_param(
            query, "verification_token"
        )

        match (maybe_provider_name, maybe_verification_token):
            case (None, None):
                error_messages.append(
                    "Missing provider and email verification token."
                )
                is_valid = False
            case (None, _):
                error_messages.append("Missing provider.")
                is_valid = False
            case (_, None):
                error_messages.append("Missing email verification token.")
                is_valid = False
            case (str(provider_name), str(verification_token)):
                try:
                    (
                        identity_id,
                        issued_at,
                        _,
                        maybe_challenge,
                        maybe_redirect_to,
                    ) = self._get_data_from_verification_token(
                        verification_token
                    )
                    await self._try_verify_email(
                        provider=provider_name,
                        issued_at=issued_at,
                        identity_id=identity_id,
                    )

                    match maybe_challenge:
                        case str(ch):
                            await pkce.create(self.db, ch)
                            maybe_pkce_code = (
                                await pkce.link_identity_challenge(
                                    self.db,
                                    identity_id,
                                    ch,
                                )
                            )
                        case _:
                            maybe_pkce_code = None

                    redirect_to = maybe_redirect_to or redirect_to
                    redirect_to = (
                        util.join_url_params(
                            redirect_to,
                            {
                                "code": maybe_pkce_code,
                            },
                        )
                        if maybe_pkce_code
                        else redirect_to
                    )

                except errors.VerificationTokenExpired:
                    app_details_config = self._get_app_details_config()
                    response.status = http.HTTPStatus.OK
                    response.content_type = b"text/html"
                    response.body = ui.render_email_verification_expired_page(
                        verification_token=verification_token,
                        app_name=app_details_config.app_name,
                        logo_url=app_details_config.logo_url,
                        dark_logo_url=app_details_config.dark_logo_url,
                        brand_color=app_details_config.brand_color,
                    )
                    return

                except Exception as ex:
                    error_messages.append(repr(ex))
                    is_valid = False

        # Only redirect back if verification succeeds
        if is_valid:
            return self._try_redirect(response, redirect_to)

        app_details_config = self._get_app_details_config()
        response.status = http.HTTPStatus.OK
        response.content_type = b'text/html'
        response.body = ui.render_email_verification_page(
            verification_token=maybe_verification_token,
            is_valid=is_valid,
            error_messages=error_messages,
            app_name=app_details_config.app_name,
            logo_url=app_details_config.logo_url,
            dark_logo_url=app_details_config.dark_logo_url,
            brand_color=app_details_config.brand_color,
        )

    async def handle_ui_resend_verification(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        query = urllib.parse.parse_qs(
            request.url.query.decode("ascii") if request.url.query else ""
        )
        ui_config = self._get_ui_config()
        password_provider = (
            self._get_password_provider() if ui_config is not None else None
        )
        is_valid = True

        if password_provider is None:
            response.status = http.HTTPStatus.NOT_FOUND
            response.body = b'Password provider not configured'
            return
        try:
            _check_keyset(query, {"verification_token"})
            verification_token = query["verification_token"][0]
            (
                identity_id,
                _,
                _,
                maybe_challenge,
                maybe_redirect_to,
            ) = self._get_data_from_verification_token(verification_token)
            email_password_client = email_password.Client(self.db)
            email_factor = (
                await email_password_client.get_email_factor_by_identity_id(
                    identity_id=identity_id
                )
            )
            if email_factor is None:
                raise errors.NoIdentityFound(
                    "Could not find email for provided identity"
                )

            verify_url = f"{self.base_path}/ui/verify"
            verification_token = self._make_verification_token(
                identity_id=identity_id,
                verify_url=verify_url,
                maybe_challenge=maybe_challenge,
                maybe_redirect_to=maybe_redirect_to,
            )

            await self._send_verification_email(
                provider=password_provider.name,
                verification_token=verification_token,
                to_addr=email_factor.email,
                verify_url=verify_url,
            )
        except Exception:
            is_valid = False

        app_details_config = self._get_app_details_config()
        response.status = http.HTTPStatus.OK
        response.content_type = b"text/html"
        response.body = ui.render_resend_verification_done_page(
            is_valid=is_valid,
            verification_token=_maybe_get_search_param(
                query, "verification_token"
            ),
            app_name=app_details_config.app_name,
            logo_url=app_details_config.logo_url,
            dark_logo_url=app_details_config.dark_logo_url,
            brand_color=app_details_config.brand_color,
        )

    async def handle_ui_magic_link_sent(
        self,
        request: protocol.HttpRequest,
        response: protocol.HttpResponse,
    ) -> None:
        """
        Success page for when a magic link is sent
        """

        app_details = self._get_app_details_config()
        response.status = http.HTTPStatus.OK
        response.content_type = b"text/html"
        response.body = ui.render_magic_link_sent_page(
            app_name=app_details.app_name,
            logo_url=app_details.logo_url,
            dark_logo_url=app_details.dark_logo_url,
            brand_color=app_details.brand_color,
        )

    def _get_webhook_config(self) -> list[config.WebhookConfig]:
        raw_webhook_configs = util.get_config(
            self.db,
            "ext::auth::AuthConfig::webhooks",
            frozenset,
        )
        return [
            config.WebhookConfig(
                events=raw_config.events,
                url=raw_config.url,
                signing_secret_key=raw_config.signing_secret_key,
            )
            for raw_config in raw_webhook_configs
        ]

    async def _maybe_send_webhook(self, event: webhook.Event) -> None:
        webhook_configs = self._get_webhook_config()
        for webhook_config in webhook_configs:
            if event.event_type in webhook_config.events:
                request_id = await webhook.send(
                    db=self.db,
                    url=webhook_config.url,
                    secret=webhook_config.signing_secret_key,
                    event=event,
                )
                logger.info(
                    f"Sent webhook request {request_id} "
                    f"to {webhook_config.url} for event {event!r}"
                )

    def _get_callback_url(self) -> str:
        return f"{self.base_path}/callback"

    def _get_auth_signing_key(self) -> jwk.JWK:
        auth_signing_key = util.get_config(
            self.db, "ext::auth::AuthConfig::auth_signing_key"
        )
        key_bytes = base64.b64encode(auth_signing_key.encode())

        return jwk.JWK(kty="oct", k=key_bytes.decode())

    def _make_state_claims(
        self,
        provider: str,
        redirect_to: str,
        redirect_to_on_signup: Optional[str],
        challenge: str,
    ) -> str:
        signing_key = self._get_auth_signing_key()
        expires_at = datetime.datetime.now(
            datetime.timezone.utc
        ) + datetime.timedelta(minutes=5)

        state_claims = {
            "iss": self.base_path,
            "provider": provider,
            "exp": expires_at.timestamp(),
            "redirect_to": redirect_to,
            "challenge": challenge,
        }
        if redirect_to_on_signup:
            state_claims['redirect_to_on_signup'] = redirect_to_on_signup
        state_token = jwt.JWT(
            header={"alg": "HS256"},
            claims=state_claims,
        )
        state_token.make_signed_token(signing_key)
        return cast(str, state_token.serialize())

    def _make_session_token(self, identity_id: str) -> str:
        signing_key = self._get_auth_signing_key()
        auth_expiration_time = util.get_config(
            self.db,
            "ext::auth::AuthConfig::token_time_to_live",
            statypes.Duration,
        )
        expires_in = auth_expiration_time.to_timedelta()
        expires_at = datetime.datetime.now(datetime.timezone.utc) + expires_in

        claims: dict[str, Any] = {
            "iss": self.base_path,
            "sub": identity_id,
        }
        if expires_in.total_seconds() != 0:
            claims["exp"] = expires_at.timestamp()
        session_token = jwt.JWT(
            header={"alg": "HS256"},
            claims=claims,
        )
        session_token.make_signed_token(signing_key)
        metrics.auth_successful_logins.inc(1.0, self.tenant.get_instance_name())
        return cast(str, session_token.serialize())

    def _get_from_claims(self, state: str, key: str) -> str:
        signing_key = self._get_auth_signing_key()
        try:
            state_token = jwt.JWT(key=signing_key, jwt=state)
        except Exception:
            raise errors.InvalidData("Invalid state token")
        state_claims: dict[str, str] = json.loads(state_token.claims)
        value = state_claims.get(key)
        if value is None:
            raise errors.InvalidData("Invalid state token")
        return value

    def _make_secret_token(
        self,
        identity_id: str,
        secret: str,
        derive_for_info: str,
        additional_claims: (
            dict[str, str | int | float | bool | None] | None
        ) = None,
        expires_in: datetime.timedelta | None = None,
    ) -> str:
        input_key_material = self._get_auth_signing_key()
        signing_key = util.derive_key(input_key_material, derive_for_info)
        expires_in = (
            datetime.timedelta(minutes=10) if expires_in is None else expires_in
        )
        return util.make_token(
            signing_key=signing_key,
            subject=identity_id,
            issuer=self.base_path,
            expires_in=expires_in,
            additional_claims={
                "jti": secret,
                **(additional_claims or {}),
            },
        )

    def _verify_and_extract_claims(
        self, jwtStr: str, key_info: str | None = None
    ) -> dict[str, str | int | float | bool]:
        input_key_material = self._get_auth_signing_key()
        if key_info is None:
            signing_key = input_key_material
        else:
            signing_key = util.derive_key(input_key_material, key_info)
        verified = jwt.JWT(key=signing_key, jwt=jwtStr)
        return cast(
            dict[str, str | int | float | bool], json.loads(verified.claims)
        )

    def _get_data_from_magic_link_token(
        self, token: str
    ) -> tuple[str, str, str]:
        try:
            claims = self._verify_and_extract_claims(token, "magic_link")
        except Exception:
            raise errors.InvalidData("Invalid 'magic_link_token'")

        identity_id = cast(Optional[str], claims.get('sub'))
        challenge = cast(Optional[str], claims.get('challenge'))
        callback_url = cast(Optional[str], claims.get('callback_url'))
        if identity_id is None or challenge is None or callback_url is None:
            raise errors.InvalidData("Invalid 'magic_link_token'")

        return (identity_id, challenge, callback_url)

    def _get_data_from_reset_token(self, token: str) -> Tuple[str, str, str]:
        try:
            claims = self._verify_and_extract_claims(token, "reset")
        except Exception:
            raise errors.InvalidData("Invalid 'reset_token'")

        identity_id = cast(Optional[str], claims.get('sub'))
        secret = cast(Optional[str], claims.get('jti'))
        challenge = cast(Optional[str], claims.get("challenge"))

        if identity_id is None or secret is None or challenge is None:
            raise errors.InvalidData("Invalid 'reset_token'")

        return (identity_id, secret, challenge)

    def _get_data_from_verification_token(
        self, token: str
    ) -> Tuple[str, float, str, Optional[str], Optional[str]]:
        try:
            claims = self._verify_and_extract_claims(token, "verify")
        except Exception:
            raise errors.InvalidData("Invalid 'verification_token'")

        identity_id = claims["sub"]
        maybe_challenge = claims.get("challenge")
        if maybe_challenge is not None and not isinstance(maybe_challenge, str):
            raise errors.InvalidData(
                "Invalid 'challenge' in 'verification_token'"
            )

        verify_url = claims.get("verify_url")
        if not isinstance(verify_url, str):
            raise errors.InvalidData(
                "Invalid 'verify_url' in 'verification_token'"
            )

        maybe_redirect_to = claims.get("redirect_to")
        if maybe_redirect_to is not None and not isinstance(
            maybe_redirect_to, str
        ):
            raise errors.InvalidData(
                "Invalid 'redirect_to' in 'verification_token'"
            )

        maybe_issued_at = claims.get("iat")
        if maybe_issued_at is None:
            raise errors.InvalidData("Missing 'iat' in 'verification_token'")

        return_value: Tuple[str, float, str, Optional[str], Optional[str]]
        match (
            identity_id,
            maybe_issued_at,
            verify_url,
            maybe_challenge,
            maybe_redirect_to,
        ):
            case (
                str(id),
                float(issued_at),
                verify_url,
                challenge,
                redirect_to,
            ):
                return_value = (
                    id,
                    issued_at,
                    verify_url,
                    challenge,
                    redirect_to,
                )
            case (_, _, _, _, _):
                raise errors.InvalidData(
                    "Invalid claims in 'verification_token'"
                )
        return return_value

    def _get_data_from_request(
        self,
        request: protocol.HttpRequest,
    ) -> dict[Any, Any]:
        content_type = request.content_type
        match content_type:
            case b"application/x-www-form-urlencoded":
                return {
                    k: v[0]
                    for k, v in urllib.parse.parse_qs(
                        request.body.decode('ascii')
                    ).items()
                }
            case b"application/json":
                data = json.loads(request.body)
                if not isinstance(data, dict):
                    raise errors.InvalidData(
                        f"Invalid json data, expected an object"
                    )
                return data
            case _:
                raise errors.InvalidData(
                    f"Unsupported Content-Type: {content_type!r}"
                )

    def _get_ui_config(self) -> config.UIConfig:
        return cast(
            config.UIConfig,
            util.maybe_get_config(
                self.db, "ext::auth::AuthConfig::ui", CompositeConfigType
            ),
        )

    def _get_app_details_config(self) -> config.AppDetailsConfig:
        return util.get_app_details_config(self.db)

    def _get_password_provider(self) -> Optional[config.ProviderConfig]:
        providers = cast(
            list[config.ProviderConfig],
            util.get_config(
                self.db,
                "ext::auth::AuthConfig::providers",
                frozenset,
            ),
        )
        password_providers = [
            p for p in providers if (p.name == 'builtin::local_emailpassword')
        ]

        return password_providers[0] if len(password_providers) == 1 else None

    def _get_webauthn_provider(self) -> config.WebAuthnProvider | None:
        providers = cast(
            list[config.ProviderConfig],
            util.get_config(
                self.db,
                "ext::auth::AuthConfig::providers",
                frozenset,
            ),
        )
        webauthn_providers = cast(
            list[config.WebAuthnProviderConfig],
            [p for p in providers if (p.name == 'builtin::local_webauthn')],
        )

        if len(webauthn_providers) == 1:
            provider = webauthn_providers[0]
            return config.WebAuthnProvider(
                name=provider.name,
                relying_party_origin=provider.relying_party_origin,
                require_verification=provider.require_verification,
            )
        else:
            return None

    def _make_verification_token(
        self,
        identity_id: str,
        verify_url: str,
        maybe_challenge: str | None,
        maybe_redirect_to: str | None,
    ) -> str:
        if not self._is_url_allowed(verify_url):
            raise errors.InvalidData(
                "Verify URL does not match any allowed URLs.",
            )

        issued_at = datetime.datetime.now(datetime.timezone.utc).timestamp()
        return self._make_secret_token(
            identity_id=identity_id,
            secret=str(uuid.uuid4()),
            derive_for_info="verify",
            additional_claims={
                "iat": issued_at,
                "challenge": maybe_challenge,
                "redirect_to": maybe_redirect_to,
                "verify_url": verify_url,
            },
            expires_in=datetime.timedelta(seconds=0),
        )

    async def _send_verification_email(
        self,
        *,
        verification_token: str,
        verify_url: str,
        provider: str,
        to_addr: str,
    ) -> None:
        await auth_emails.send_verification_email(
            db=self.db,
            tenant=self.tenant,
            to_addr=to_addr,
            verification_token=verification_token,
            provider=provider,
            verify_url=verify_url,
            test_mode=self.test_mode,
        )

    async def _try_verify_email(
        self, provider: str, issued_at: float, identity_id: str
    ) -> EmailFactor:
        current_time = datetime.datetime.now(datetime.timezone.utc)
        issued_at_datetime = datetime.datetime.fromtimestamp(
            issued_at, datetime.timezone.utc
        )
        token_age = current_time - issued_at_datetime
        if token_age > datetime.timedelta(hours=24):
            raise errors.VerificationTokenExpired()

        client: email_password.Client | webauthn.Client
        match provider:
            case "builtin::local_emailpassword":
                client = email_password.Client(db=self.db)
            case "builtin::local_webauthn":
                client = webauthn.Client(self.db)
            case _:
                raise errors.InvalidData(
                    f"Unknown provider: {provider}",
                )

        updated = await client.verify_email(identity_id, current_time)
        if updated is None:
            raise errors.NoIdentityFound(
                "Could not verify email for identity"
                f" {identity_id}. This email address may not exist"
                " in our system, or it might already be verified."
            )
        return updated

    def _is_url_allowed(self, url: str) -> bool:
        allowed_urls = util.get_config(
            self.db,
            "ext::auth::AuthConfig::allowed_redirect_urls",
            frozenset,
        )
        allowed_urls = cast(FrozenSet[str], allowed_urls).union(
            {self.base_path}
        )

        ui_config = self._get_ui_config()
        if ui_config:
            allowed_urls = allowed_urls.union({ui_config.redirect_to})
            if ui_config.redirect_to_on_signup:
                allowed_urls = allowed_urls.union(
                    {ui_config.redirect_to_on_signup}
                )

        lower_url = url.lower()

        for allowed_url in allowed_urls:
            lower_allowed_url = allowed_url.lower()
            if lower_url.startswith(lower_allowed_url):
                return True

            parsed_allowed_url = urllib.parse.urlparse(lower_allowed_url)
            allowed_domain = parsed_allowed_url.netloc
            allowed_path = parsed_allowed_url.path

            parsed_lower_url = urllib.parse.urlparse(lower_url)
            lower_domain = parsed_lower_url.netloc
            lower_path = parsed_lower_url.path

            if (
                lower_domain == allowed_domain
                or lower_domain.endswith('.' + allowed_domain)
            ) and lower_path.startswith(allowed_path):
                return True

        return False

    def _do_redirect(
        self, response: protocol.HttpResponse, allowed_url: AllowedUrl
    ) -> None:
        response.status = http.HTTPStatus.FOUND
        response.custom_headers["Location"] = allowed_url.url

    def _try_redirect(self, response: protocol.HttpResponse, url: str) -> None:
        allowed_url = self._make_allowed_url(url)
        self._do_redirect(response, allowed_url)

    def _make_allowed_url(self, url: str) -> AllowedUrl:
        if not self._is_url_allowed(url):
            raise errors.InvalidData(
                "Redirect URL does not match any allowed URLs.",
            )
        return AllowedUrl(url)

    def _maybe_make_allowed_url(
        self, url: Optional[str]
    ) -> Optional[AllowedUrl]:
        return self._make_allowed_url(url) if url else None


@dataclasses.dataclass
class AllowedUrl:
    url: str

    def map(self, f: Callable[[str], str]) -> "AllowedUrl":
        return AllowedUrl(f(self.url))


def _fail_with_error(
    *,
    response: protocol.HttpResponse,
    status: http.HTTPStatus,
    ex: Exception,
) -> None:
    err_dct = {
        "message": str(ex),
        "type": str(ex.__class__.__name__),
    }

    logger.error(f"Failed to handle HTTP request: {err_dct!r}")
    response.body = json.dumps({"error": err_dct}).encode()
    response.status = status


def _maybe_get_search_param(
    query_dict: dict[str, list[str]], key: str
) -> str | None:
    params = query_dict.get(key)
    return params[0] if params else None


def _get_search_param(
    query_dict: dict[str, list[str]],
    key: str,
    *,
    fallback_keys: Optional[list[str]] = None,
) -> str:
    val = _maybe_get_search_param(query_dict, key)
    if val is None and fallback_keys is not None:
        for fallback_key in fallback_keys:
            val = _maybe_get_search_param(query_dict, fallback_key)
            if val is not None:
                break
    if val is None:
        raise errors.InvalidData(f"Missing query parameter: {key}")
    return val


def _maybe_get_form_field(
    form_dict: dict[str, list[str]], key: str
) -> str | None:
    maybe_val = form_dict.get(key)
    if maybe_val is None:
        return None
    return maybe_val[0]


def _get_pkce_challenge(
    *,
    response: protocol.HttpResponse,
    cookies: http.cookies.SimpleCookie,
    query_dict: dict[str, list[str]],
) -> str | None:
    cookie_name = 'edgedb-pkce-challenge'
    challenge: str | None = _maybe_get_search_param(
        query_dict, 'challenge'
    ) or _maybe_get_search_param(query_dict, "code_challenge")
    if challenge is not None:
        _set_cookie(response, cookie_name, challenge)
    else:
        if 'edgedb-pkce-challenge' in cookies:
            challenge = cookies['edgedb-pkce-challenge'].value
    return challenge


def _set_cookie(
    response: protocol.HttpResponse,
    name: str,
    value: str,
    *,
    http_only: bool = True,
    secure: bool = True,
    same_site: str = "Strict",
    path: Optional[str] = None,
) -> None:
    val: http.cookies.Morsel[str] = http.cookies.SimpleCookie({name: value})[
        name
    ]
    val["httponly"] = http_only
    val["secure"] = secure
    val["samesite"] = same_site
    if path is not None:
        val["path"] = path
    response.custom_headers["Set-Cookie"] = val.OutputString()


def _check_keyset(candidate: dict[str, Any], keyset: set[str]) -> None:
    missing_fields = [field for field in keyset if field not in candidate]
    if missing_fields:
        raise errors.InvalidData(
            f"Missing required fields: {', '.join(missing_fields)}"
        )
