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

from typing import Any
from edb.server.protocol import execute


class Client:
    def __init__(self, db: Any):
        self.db = db

    async def verify_email(
        self, identity_id: str, verified_at: datetime.datetime
    ):
        r = await execute.parse_execute_json(
            db=self.db,
            query="""\
with
    identity_id := <uuid><str>$identity_id,
    verified_at := <datetime>$verified_at,
update ext::auth::EmailFactor
filter .identity.id = identity_id
    and not exists .verified_at ?? false
set { verified_at := verified_at };""",
            variables={
                "identity_id": identity_id,
                "verified_at": verified_at.isoformat(),
            },
            cached_globally=True,
        )

        return r

    async def get_email_by_identity_id(self, identity_id: str) -> str | None:
        r = await execute.parse_execute_json(
            self.db,
            """
select ext::auth::EmailFactor {
    email,
} filter .identity.id = <uuid>$identity_id;
            """,
            variables={"identity_id": identity_id},
            cached_globally=True,
        )

        result_json = json.loads(r.decode())
        if len(result_json) == 0:
            return None

        assert len(result_json) == 1

        return result_json[0]["email"]

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
        )

        result_json = json.loads(r.decode())
        if len(result_json) == 0:
            return None

        assert len(result_json) == 1

        return result_json[0]["verified_at"]
