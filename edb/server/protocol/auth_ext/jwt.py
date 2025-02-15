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

import dataclasses
import datetime

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDFExpand
from cryptography.hazmat.backends import default_backend

from typing import Any, Callable

from edb.server import auth as jwt_auth
from edb.ir import statypes as statypes

from . import errors


VALIDATION_TOKEN_DEFAULT_EXPIRATION = datetime.timedelta(seconds=24 * 60 * 60)
RESET_TOKEN_DEFAULT_EXPIRATION = datetime.timedelta(minutes=10)
OAUTH_STATE_TOKEN_DEFAULT_EXPIRATION = datetime.timedelta(minutes=5)


class SigningKey:
    subkeys: dict[str | None, jwt_auth.JWKSet]

    def __init__(
        self,
        key_fetch: Callable[[], str],
        issuer: str,
        *,
        is_key_for_testing: bool = False,
    ):
        self.key = ""
        self.key_fetch = key_fetch
        self.issuer = issuer
        self.subkeys = {}
        self.__is_key_for_testing = is_key_for_testing

    def subkey(self, info: str | None = None) -> jwt_auth.JWKSet:
        # Clear keycache if the key has changed
        current_key = self.key_fetch()
        if current_key != self.key:
            self.key = current_key
            self.subkeys = {}

        if info in self.subkeys:
            return self.subkeys[info]
        if info is None:
            key = jwt_auth.JWKSet.from_hs256_key(self.key.encode())
        else:
            key = jwt_auth.JWKSet.from_hs256_key(derive_key_raw(self.key, info))
        key.default_validation_context.require_expiry()
        key.default_validation_context.allow("iss", [self.issuer])
        self.subkeys[info] = key
        return key

    def sign(
        self,
        info: str | None,
        claims: dict[str, str | None],
        *,
        ctx: jwt_auth.SigningCtx,
    ) -> str:
        # Remove any None values from the claims
        claims = {k: v for k, v in claims.items() if v is not None}
        if self.__is_key_for_testing:
            claims["__test__"] = str(info)
        return self.subkey(info).sign(claims, ctx=ctx)

    def validate(
        self,
        token: str,
        info: str | None = None,
        skip_expiration_check: bool = False,
    ) -> dict[str, Any]:
        key = self.subkey(info)

        try:
            ctx = None
            if skip_expiration_check:
                ctx = jwt_auth.ValidationCtx()
                ctx.ignore_expiry()
                ctx.allow("iss", [self.issuer])
            return key.validate(token, ctx=ctx)
        except Exception as e:
            raise errors.InvalidData(f"Invalid token: {e}") from e


def verify_str(cls: Any, claims: dict[str, Any], key: str) -> str:
    value = claims.get(key, None)
    if isinstance(value, str):
        return value
    raise errors.InvalidData(f"Invalid '{cls.__name__}'")


def verify_str_opt(cls: Any, claims: dict[str, Any], key: str) -> str | None:
    value = claims.get(key, None)
    if isinstance(value, str):
        return value
    if value is None:
        return None
    raise errors.InvalidData(f"Invalid '{cls.__name__}'")


@dataclasses.dataclass
class MagicLinkToken:
    """
    A token that can be used to verify a magic link sent to a user via email.

    Expiration is controlled by the provider parameter `token_time_to_live`.
    """
    subject: str
    callback_url: str
    challenge: str

    def sign(
        self, signing_key: SigningKey, expires_in: datetime.timedelta
    ) -> str:
        signing_ctx = jwt_auth.SigningCtx()
        signing_ctx.set_expiry(int(expires_in.total_seconds()))
        signing_ctx.set_not_before(30)
        signing_ctx.set_issuer(signing_key.issuer)
        return signing_key.sign(
            "magic_link",
            {
                "sub": self.subject,
                "callback_url": self.callback_url,
                "challenge": self.challenge,
            },
            ctx=signing_ctx,
        )

    @classmethod
    def verify(cls, token: str, signing_key: SigningKey) -> 'MagicLinkToken':
        claims = signing_key.validate(token, "magic_link")

        identity_id = verify_str(cls, claims, 'sub')
        challenge = verify_str(cls, claims, 'challenge')
        callback_url = verify_str(cls, claims, 'callback_url')

        return MagicLinkToken(
            subject=identity_id,
            callback_url=callback_url,
            challenge=challenge,
        )


@dataclasses.dataclass
class ResetToken:
    """
    A token that can be used to verify a password reset request.
    """
    subject: str
    secret: str
    challenge: str

    def sign(
        self,
        signing_key: SigningKey,
        expires_in: datetime.timedelta = RESET_TOKEN_DEFAULT_EXPIRATION,
    ) -> str:
        signing_ctx = jwt_auth.SigningCtx()
        signing_ctx.set_expiry(int(expires_in.total_seconds()))
        signing_ctx.set_not_before(30)
        signing_ctx.set_issuer(signing_key.issuer)
        return signing_key.sign(
            "reset",
            {
                "sub": self.subject,
                "secret": self.secret,
                "challenge": self.challenge,
            },
            ctx=signing_ctx,
        )

    @classmethod
    def verify(cls, token: str, signing_key: SigningKey) -> 'ResetToken':
        claims = signing_key.validate(token, "reset")

        return ResetToken(
            subject=verify_str(cls, claims, 'sub'),
            secret=verify_str(cls, claims, 'secret'),
            challenge=verify_str(cls, claims, 'challenge'),
        )


@dataclasses.dataclass
class VerificationToken:
    """
    A token that can be used to verify a user's email address. Note that we
    allow expired tokens to trigger a resend of the verification email, but not
    to verify the email address.
    """
    subject: str
    verify_url: str
    maybe_challenge: str | None
    maybe_redirect_to: str | None

    def sign(
        self,
        signing_key: SigningKey,
        expires_in: datetime.timedelta = VALIDATION_TOKEN_DEFAULT_EXPIRATION,
    ) -> str:
        signing_ctx = jwt_auth.SigningCtx()
        signing_ctx.set_expiry(int(expires_in.total_seconds()))
        signing_ctx.set_not_before(30)
        signing_ctx.set_issuer(signing_key.issuer)
        return signing_key.sign(
            "verification",
            {
                "sub": self.subject,
                "verify_url": self.verify_url,
                "challenge": self.maybe_challenge,
                "redirect_to": self.maybe_redirect_to,
            },
            ctx=signing_ctx,
        )

    @classmethod
    def verify(
        cls,
        token: str,
        signing_key: SigningKey,
        skip_expiration_check: bool = False,
    ) -> 'VerificationToken':
        claims = signing_key.validate(
            token,
            "verification",
            skip_expiration_check=skip_expiration_check,
        )

        return VerificationToken(
            subject=verify_str(cls, claims, 'sub'),
            verify_url=verify_str(cls, claims, 'verify_url'),
            maybe_challenge=verify_str_opt(cls, claims, 'challenge'),
            maybe_redirect_to=verify_str_opt(cls, claims, 'redirect_to'),
        )


@dataclasses.dataclass
class SessionToken:
    """
    The token representing an auth session for a user. Expiration is controlled
    by the database parameter `ext::auth::AuthConfig::token_time_to_live`.
    """
    subject: str

    def sign(
        self,
        signing_key: SigningKey,
        expires_in: datetime.timedelta,
    ) -> str:
        signing_ctx = jwt_auth.SigningCtx()
        signing_ctx.set_expiry(int(expires_in.total_seconds()))
        signing_ctx.set_not_before(30)
        signing_ctx.set_issuer(signing_key.issuer)
        return signing_key.sign(
            None,
            {
                "sub": self.subject,
            },
            ctx=signing_ctx,
        )

    @classmethod
    def verify(cls, token: str, signing_key: SigningKey) -> 'SessionToken':
        claims = signing_key.validate(token, None)

        return SessionToken(
            subject=verify_str(cls, claims, 'sub'),
        )


@dataclasses.dataclass
class OAuthStateToken:
    """
    The token representing an OAuth state passed to the identity provider.
    """
    provider: str
    redirect_to: str
    challenge: str
    redirect_to_on_signup: str | None = None

    def sign(
        self,
        signing_key: SigningKey,
        expires_in: datetime.timedelta = OAUTH_STATE_TOKEN_DEFAULT_EXPIRATION,
    ) -> str:
        signing_ctx = jwt_auth.SigningCtx()
        signing_ctx.set_expiry(int(expires_in.total_seconds()))
        signing_ctx.set_not_before(30)
        signing_ctx.set_issuer(signing_key.issuer)
        return signing_key.sign(
            "state",
            {
                "provider": self.provider,
                "redirect_to": self.redirect_to,
                "redirect_to_on_signup": self.redirect_to_on_signup,
                "challenge": self.challenge,
            },
            ctx=signing_ctx,
        )

    @classmethod
    def verify(cls, token: str, signing_key: SigningKey) -> 'OAuthStateToken':
        claims = signing_key.validate(token, "state")

        return OAuthStateToken(
            provider=verify_str(cls, claims, 'provider'),
            redirect_to=verify_str(cls, claims, 'redirect_to'),
            redirect_to_on_signup=verify_str_opt(
                cls, claims, 'redirect_to_on_signup'
            ),
            challenge=verify_str(cls, claims, 'challenge'),
        )


def derive_key_raw(key: str, info: str) -> bytes:
    """Derive a new key from the given symmetric key using HKDF."""
    input_key_material = key.encode()

    backend = default_backend()
    hkdf = HKDFExpand(
        algorithm=hashes.SHA256(),
        length=32,
        info=info.encode("utf-8"),
        backend=backend,
    )
    new_key_bytes = hkdf.derive(input_key_material)
    return new_key_bytes
