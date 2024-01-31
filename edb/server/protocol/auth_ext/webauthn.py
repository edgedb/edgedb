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

import dataclasses
import base64
import json
import webauthn
from edb.errors import ConstraintViolationError

from edb.server.protocol import execute

from . import config, data, errors


@dataclasses.dataclass(repr=False)
class WebAuthnRegistrationChallenge:
    """
    Object that represents the ext::auth::WebAuthnRegistrationChallenge type
    """

    id: str
    challenge: bytes
    user_handle: bytes
    email: str


async def create_registration_challenge(
    db,
    *,
    email: str,
    challenge: bytes,
    user_handle: bytes,
):
    await execute.parse_execute_json(
        db,
        """
        with
            challenge := <str>$challenge,
            user_handle := <str>$user_handle,
        insert ext::auth::WebAuthnRegistrationChallenge {
            challenge := enc::base64_decode(challenge),
            user_handle := enc::base64_decode(user_handle),
            email := <str>$email,
        }""",
        variables={
            "challenge": base64.b64encode(challenge).decode(),
            "user_handle": base64.b64encode(user_handle).decode(),
            "email": email,
        },
        cached_globally=True,
    )


async def register(
    db,
    *,
    credentials: str,
    email: str,
    user_handle: bytes,
    provider_config: config.WebAuthnProvider,
):
    registration_challenge = await get_registration_challenge(
        db,
        email=email,
        user_handle=user_handle,
    )
    registration_verification = webauthn.verify_registration_response(
        credential=credentials,
        expected_challenge=registration_challenge.challenge,
        expected_rp_id=provider_config.relying_party_id,
        expected_origin=provider_config.relying_party_origin,
    )

    try:
        result = await execute.parse_execute_json(
            db,
            """
            with
                email := <str>$email,
                user_handle := <str>$user_handle,
                credential_id := <str>$credential_id,
                public_key := <str>$public_key,
                identity := (insert ext::auth::LocalIdentity {
                    issuer := "local",
                    subject := "",
                }),
                factor := (insert ext::auth::WebAuthnFactor {
                    email := <str>$email,
                    user_handle := enc::base64_decode(<str>$user_handle),
                    credential_id := enc::base64_decode(<str>$credential_id),
                    public_key := enc::base64_decode(<str>$public_key),
                    identity := identity,
                }),
            select identity { * };""",
            variables={
                "email": email,
                "user_handle": base64.b64encode(user_handle).decode(),
                "credential_id": base64.b64encode(
                    registration_verification.credential_id
                ).decode(),
                "public_key": base64.b64encode(
                    registration_verification.credential_public_key
                ).decode(),
            },
            cached_globally=True,
        )
    except Exception as e:
        exc = await execute.interpret_error(e, db)
        if isinstance(exc, ConstraintViolationError):
            raise errors.UserAlreadyRegistered()
        else:
            raise exc

    result_json = json.loads(result.decode())
    assert len(result_json) == 1

    return data.LocalIdentity(**result_json[0])


async def get_registration_challenge(
    db,
    *,
    email: str,
    user_handle: bytes,
) -> WebAuthnRegistrationChallenge:
    result = await execute.parse_execute_json(
        db,
        """
        select ext::auth::WebAuthnRegistrationChallenge {
            id,
            challenge,
            user_handle,
            email,
        }
        filter .email = <str>$email
        and .user_handle = enc::base64_decode(<str>$user_handle);
        """,
        variables={
            "email": email,
            "user_handle": base64.b64encode(user_handle).decode(),
        },
        cached_globally=True,
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
