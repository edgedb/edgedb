#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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


import datetime
import json
import base64

from jwcrypto import jwk
from typing import Any, cast
from edb.server.protocol import execute

from . import util, data


class Client:
    def __init__(self, db: Any):
        self.db = db

    def _get_signing_key(self) -> jwk.JWK:
        auth_signing_key = util.get_config(
            self.db, "ext::auth::AuthConfig::auth_signing_key"
        )
        key_bytes = base64.b64encode(auth_signing_key.encode())

        return jwk.JWK(kty="oct", k=key_bytes.decode())

    async def verify_email(
        self, identity_id: str, verified_at: datetime.datetime
    ) -> data.EmailFactor | None:
        result_bytes = await execute.parse_execute_json(
            db=self.db,
            query="""\
with
    LOCAL_IDENTITY := <ext::auth::LocalIdentity><uuid>$identity_id,
    verified_at := <datetime>$verified_at,
    UPDATED := (
        update ext::auth::EmailFactor
        filter .identity = LOCAL_IDENTITY
            and not exists .verified_at ?? false
        set { verified_at := verified_at }
    ),
select UPDATED {**};
""",
            variables={
                "identity_id": identity_id,
                "verified_at": verified_at.isoformat(),
            },
            cached_globally=True,
            query_tag='gel/auth',
        )
        result_json = json.loads(result_bytes.decode())
        if len(result_json) == 0:
            return None

        factor = result_json[0]
        return data.EmailFactor(**factor)

    async def get_email_factor_by_identity_id(
        self, identity_id: str
    ) -> data.EmailFactor | None:
        r = await execute.parse_execute_json(
            self.db,
            """
select ext::auth::EmailFactor { ** } filter .identity.id = <uuid>$identity_id;
            """,
            variables={"identity_id": identity_id},
            cached_globally=True,
            query_tag='gel/auth',
        )

        result_json = json.loads(r.decode())
        if len(result_json) == 0:
            return None

        assert len(result_json) == 1

        factor = result_json[0]

        return data.EmailFactor(**factor)

    async def get_verified_by_identity_id(self, identity_id: str) -> str | None:
        r = await execute.parse_execute_json(
            self.db,
            """
select ext::auth::EmailFactor {
    verified_at,
} filter .identity.id = <uuid>$identity_id;
            """,
            variables={"identity_id": identity_id},
            cached_globally=True,
            query_tag='gel/auth',
        )

        result_json = json.loads(r.decode())
        if len(result_json) == 0:
            return None

        assert len(result_json) == 1

        return cast(str, result_json[0]["verified_at"])

    async def get_identity_id_by_email(
        self, email: str, *, factor_type: str = 'EmailFactor'
    ) -> str | None:
        r = await execute.parse_execute_json(
            self.db,
            f"""
with
    email := <str>$email,
    identity := (
        select ext::auth::LocalIdentity
        filter .<identity[is ext::auth::{factor_type}].email = email
    ),
select identity.id;""",
            variables={"email": email},
            cached_globally=True,
            query_tag='gel/auth',
        )

        result_json = json.loads(r.decode())
        if len(result_json) == 0:
            return None

        assert len(result_json) == 1

        return cast(str, result_json[0])

    async def get_email_factor_by_email(
        self, email: str
    ) -> data.EmailFactor | None:
        r = await execute.parse_execute_json(
            self.db,
            """
select ext::auth::EmailFactor { ** } filter .email = <str>$email;
            """,
            variables={"email": email},
            cached_globally=True,
            query_tag='gel/auth',
        )

        result_json = json.loads(r.decode())
        if len(result_json) == 0:
            return None

        assert len(result_json) == 1

        factor = result_json[0]
        return data.EmailFactor(**factor)
