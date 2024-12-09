#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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

import dataclasses
import base64
import json
import webauthn

from typing import Optional, Tuple, TYPE_CHECKING
from webauthn.helpers import (
    parse_authentication_credential_json,
    structs as webauthn_structs,
    exceptions as webauthn_exceptions,
)

from edb.errors import ConstraintViolationError
from edb.server.protocol import execute

from . import config, data, errors, util, local

if TYPE_CHECKING:
    from edb.server import tenant as edbtenant


@dataclasses.dataclass(repr=False)
class WebAuthnRegistrationChallenge:
    """
    Object that represents the ext::auth::WebAuthnRegistrationChallenge type
    """

    id: str
    challenge: bytes
    user_handle: bytes
    email: str


class Client(local.Client):
    def __init__(self, db: edbtenant.dbview.Database):
        self.db = db
        self.provider = self._get_provider()
        self.app_name = self._get_app_name()

    def _get_provider(self) -> config.WebAuthnProvider:
        provider_name = "builtin::local_webauthn"
        provider_client_config = util.get_config(
            self.db, "ext::auth::AuthConfig::providers", frozenset
        )
        for cfg in provider_client_config:
            if cfg.name == provider_name:
                return config.WebAuthnProvider(
                    name=cfg.name,
                    relying_party_origin=cfg.relying_party_origin,
                    require_verification=cfg.require_verification,
                )

        raise errors.MissingConfiguration(
            provider_name, f"Provider is not configured"
        )

    def _get_app_name(self) -> Optional[str]:
        app_config = util.get_app_details_config(self.db)
        return app_config.app_name

    async def create_registration_options_for_email(
        self, email: str,
    ) -> tuple[str, bytes]:
        maybe_user_handle = await self._maybe_get_existing_user_handle(
            email=email
        )
        registration_options = webauthn.generate_registration_options(
            rp_id=self.provider.relying_party_id,
            rp_name=(self.app_name or self.provider.relying_party_origin),
            user_name=email,
            user_display_name=email,
            user_id=maybe_user_handle,
        )

        await self._create_registration_challenge(
            email=email,
            challenge=registration_options.challenge,
            user_handle=registration_options.user.id,
        )

        return (
            base64.urlsafe_b64encode(registration_options.user.id).decode(),
            webauthn.options_to_json(registration_options).encode(),
        )

    async def _maybe_get_existing_user_handle(
        self, email: str,
    ) -> Optional[bytes]:
        result = await execute.parse_execute_json(
            self.db,
            """
with
    email := <str>$email,
    factors := (
        select ext::auth::WebAuthnFactor
        filter .email = email
    ),
select assert_single((select distinct factors.user_handle));""",
            variables={
                "email": email,
            },
            cached_globally=True,
            query_tag='gel/auth',
        )

        result_json = json.loads(result.decode())
        if len(result_json) == 0:
            return None
        else:
            return base64.b64decode(result_json[0])

    async def _create_registration_challenge(
        self,
        email: str,
        challenge: bytes,
        user_handle: bytes,
    ) -> None:
        await execute.parse_execute_json(
            self.db,
            """
with
    challenge := <bytes>$challenge,
    user_handle := <bytes>$user_handle,
    email := <str>$email,
insert ext::auth::WebAuthnRegistrationChallenge {
    challenge := challenge,
    user_handle := user_handle,
    email := email,
}""",
            variables={
                "challenge": challenge,
                "user_handle": user_handle,
                "email": email,
            },
            cached_globally=True,
            query_tag='gel/auth',
        )

    async def register(
        self,
        credentials: str,
        email: str,
        user_handle: bytes,
    ) -> data.EmailFactor:
        registration_challenge = await self._get_registration_challenge(
            email=email,
            user_handle=user_handle,
        )
        await self._delete_registration_challenges(
            email=email,
            user_handle=user_handle,
        )

        registration_verification = webauthn.verify_registration_response(
            credential=credentials,
            expected_challenge=registration_challenge.challenge,
            expected_rp_id=self.provider.relying_party_id,
            expected_origin=self.provider.relying_party_origin,
        )

        try:
            result = await execute.parse_execute_json(
                self.db,
                """
with
    email := <str>$email,
    user_handle := <bytes>$user_handle,
    credential_id := <bytes>$credential_id,
    public_key := <bytes>$public_key,
    identity := (insert ext::auth::LocalIdentity {
        issuer := "local",
        subject := "",
    }),
    factor := (insert ext::auth::WebAuthnFactor {
        email := email,
        user_handle := user_handle,
        credential_id := credential_id,
        public_key := public_key,
        identity := identity,
    }),
select factor { ** };""",
                variables={
                    "email": email,
                    "user_handle": user_handle,
                    "credential_id": registration_verification.credential_id,
                    "public_key": (
                        registration_verification.credential_public_key
                    ),
                },
                cached_globally=True,
                query_tag='gel/auth',
            )
        except Exception as e:
            exc = await execute.interpret_error(e, self.db)
            if isinstance(exc, ConstraintViolationError):
                raise errors.UserAlreadyRegistered()
            else:
                raise exc

        result_json = json.loads(result.decode())
        assert len(result_json) == 1

        factor_dict = result_json[0]
        local_identity = data.LocalIdentity(**factor_dict["identity"])
        return data.WebAuthnFactor(**factor_dict, identity=local_identity)

    async def _get_registration_challenge(
        self,
        email: str,
        user_handle: bytes,
    ) -> WebAuthnRegistrationChallenge:
        result = await execute.parse_execute_json(
            self.db,
            """
with
    email := <str>$email,
    user_handle := <bytes>$user_handle,
select ext::auth::WebAuthnRegistrationChallenge {
    id,
    challenge,
    user_handle,
    email,
}
filter .email = email and .user_handle = user_handle;""",
            variables={
                "email": email,
                "user_handle": user_handle,
            },
            cached_globally=True,
            query_tag='gel/auth',
        )
        result_json = json.loads(result.decode())
        assert len(result_json) == 1
        challenge_dict = result_json[0]

        return WebAuthnRegistrationChallenge(
            id=challenge_dict["id"],
            challenge=base64.b64decode(challenge_dict["challenge"]),
            user_handle=base64.b64decode(challenge_dict["user_handle"]),
            email=challenge_dict["email"],
        )

    async def _delete_registration_challenges(
        self,
        email: str,
        user_handle: bytes,
    ) -> None:
        await execute.parse_execute_json(
            self.db,
            """
with
    email := <str>$email,
    user_handle := <bytes>$user_handle,
delete ext::auth::WebAuthnRegistrationChallenge
filter .email = email and .user_handle = user_handle;""",
            variables={
                "email": email,
                "user_handle": user_handle,
            },
            query_tag='gel/auth',
        )

    async def create_authentication_options_for_email(
        self,
        *,
        webauthn_provider: config.WebAuthnProvider,
        email: str,
    ) -> Tuple[str, bytes]:
        # Find credential IDs by email
        result = await execute.parse_execute_json(
            self.db,
            """
select ext::auth::WebAuthnFactor {
    user_handle,
    credential_id,
}
filter .email = <str>$email;""",
            variables={
                "email": email,
            },
            cached_globally=True,
            query_tag='gel/auth',
        )
        result_json = json.loads(result.decode())
        if len(result_json) == 0:
            raise errors.WebAuthnAuthenticationFailed(
                "No WebAuthn credentials found for this email."
            )

        user_handles: set[str] = {x["user_handle"] for x in result_json}
        assert (
            len(user_handles) == 1
        ), "Found WebAuthn multiple user handles for the same email."

        user_handle = base64.b64decode(result_json[0]["user_handle"])

        credential_ids = [
            webauthn_structs.PublicKeyCredentialDescriptor(
                base64.b64decode(x["credential_id"])
            )
            for x in result_json
        ]

        registration_options = webauthn.generate_authentication_options(
            rp_id=webauthn_provider.relying_party_id,
            allow_credentials=credential_ids,
        )

        await execute.parse_execute_json(
            self.db,
            """
with
    challenge := <bytes>$challenge,
    user_handle := <bytes>$user_handle,
    email := <str>$email,
    factors := (
        assert_exists((
            select ext::auth::WebAuthnFactor
            filter .user_handle = user_handle
            and .email = email
        ))
    )
insert ext::auth::WebAuthnAuthenticationChallenge {
    challenge := challenge,
    factors := factors,
}
unless conflict on .factors
else (
    update ext::auth::WebAuthnAuthenticationChallenge
    set {
        challenge := challenge
    }
);""",
            variables={
                "challenge": registration_options.challenge,
                "user_handle": user_handle,
                "email": email,
            },
            query_tag='gel/auth',
        )

        return (
            base64.urlsafe_b64encode(user_handle).decode(),
            webauthn.options_to_json(registration_options).encode(),
        )

    async def is_email_verified(
        self,
        email: str,
        assertion: str,
    ) -> bool:
        credential = parse_authentication_credential_json(assertion)

        result = await execute.parse_execute_json(
            self.db,
            """
with
    email := <str>$email,
    credential_id := <bytes>$credential_id,
    factor := assert_single((
        select ext::auth::WebAuthnFactor
        filter .email = email
        and credential_id = credential_id
    )),
select (factor.verified_at <= std::datetime_current()) ?? false;""",
            variables={
                "email": email,
                "credential_id": credential.raw_id,
            },
            cached_globally=True,
            query_tag='gel/auth',
        )
        result_json = json.loads(result.decode())
        return bool(result_json[0])

    async def _get_authentication_challenge(
        self,
        email: str,
        credential_id: bytes,
    ) -> data.WebAuthnAuthenticationChallenge:
        result = await execute.parse_execute_json(
            self.db,
            """
with
    email := <str>$email,
    credential_id := <bytes>$credential_id,
select ext::auth::WebAuthnAuthenticationChallenge {
    id,
    created_at,
    modified_at,
    challenge,
    factors: {
        id,
        created_at,
        modified_at,
        email,
        verified_at,
        user_handle,
        credential_id,
        public_key,
        identity: {
            created_at,
            modified_at,
            id,
            issuer,
            subject,
        }
    },
}
filter .factors.email = email and .factors.credential_id = credential_id;""",
            variables={
                "email": email,
                "credential_id": credential_id,
            },
            cached_globally=True,
            query_tag='gel/auth',
        )
        result_json = json.loads(result.decode())
        if len(result_json) == 0:
            raise errors.WebAuthnAuthenticationFailed(
                "Could not find a challenge. Please retry authentication."
            )
        elif len(result_json) > 1:
            raise errors.WebAuthnAuthenticationFailed(
                "Multiple challenges found. Please retry authentication."
            )
        return data.WebAuthnAuthenticationChallenge(**result_json[0])

    async def _delete_authentication_challenges(
        self,
        email: str,
        credential_id: bytes,
    ) -> None:
        await execute.parse_execute_json(
            self.db,
            """
with
    email := <str>$email,
    credential_id := <bytes>$credential_id,
delete ext::auth::WebAuthnAuthenticationChallenge
filter .factors.email = email and .factors.credential_id = credential_id;""",
            variables={
                "email": email,
                "credential_id": credential_id,
            },
            query_tag='gel/auth',
        )

    async def authenticate(
        self,
        *,
        email: str,
        assertion: str,
    ) -> data.LocalIdentity:
        credential = parse_authentication_credential_json(assertion)

        authentication_challenge = await self._get_authentication_challenge(
            email=email,
            credential_id=credential.raw_id,
        )
        await self._delete_authentication_challenges(
            email=email,
            credential_id=credential.raw_id,
        )

        factor = next(
            (
                f
                for f in authentication_challenge.factors
                if f.credential_id == credential.raw_id
            ),
            None,
        )
        assert factor is not None, "Missing factor for the given credential."

        try:
            webauthn.verify_authentication_response(
                credential=credential,
                expected_challenge=authentication_challenge.challenge,
                credential_public_key=factor.public_key,
                credential_current_sign_count=0,
                expected_rp_id=self.provider.relying_party_id,
                expected_origin=self.provider.relying_party_origin,
            )
        except webauthn_exceptions.InvalidAuthenticationResponse:
            raise errors.WebAuthnAuthenticationFailed(
                "Invalid authentication response. Please retry authentication."
            )

        return factor.identity

    async def get_email_factor_by_credential_id(
        self,
        credential_id: bytes,
    ) -> Optional[data.EmailFactor]:
        result = await execute.parse_execute_json(
            self.db,
            """
with
    credential_id := <bytes>$credential_id,
select ext::auth::WebAuthnFactor {
    id,
    created_at,
    modified_at,
    email,
    verified_at,
    identity: {*},
} filter .credential_id = credential_id;""",
            variables={
                "credential_id": credential_id,
            },
            query_tag='gel/auth',
        )
        result_json = json.loads(result.decode())
        if len(result_json) == 0:
            return None
        elif len(result_json) > 1:
            # This should never happen given the exclusive constraint
            raise errors.WebAuthnAuthenticationFailed(
                "Multiple WebAuthn factors found for the same credential ID."
            )
        return data.EmailFactor(**result_json[0])
