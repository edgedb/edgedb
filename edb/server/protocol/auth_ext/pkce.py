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


import json
import dataclasses

from edb.server.protocol import execute


@dataclasses.dataclass(repr=False)
class PKCEChallenge:
    """
    Object that represents the ext::auth::PKCEChallenge type
    """

    id: str
    challenge: str
    identity_id: str | None


async def create(db, challenge: str):
    await execute.parse_execute_json(
        db,
        """
        insert ext::auth::PKCEChallenge {
            challenge := <str>$challenge,
        }
        """,
        variables={
            "challenge": challenge,
        },
    )


async def link_identity_challenge(db, identity_id: str, challenge: str) -> str:
    r = await execute.parse_execute_json(
        db,
        """
        update ext::auth::PKCEChallenge
        filter .challenge = <str>$challenge
        set { identity := <ext::auth::Identity><uuid>$identity_id }
        """,
        variables={
            "challenge": challenge,
            "identity_id": identity_id,
        },
    )

    result_json = json.loads(r.decode())
    assert len(result_json) == 1

    return result_json[0]["id"]


async def get_by_id(db, id: str) -> PKCEChallenge:
    r = await execute.parse_execute_json(
        db,
        """
        select ext::auth::PKCEChallenge {
            id,
            challenge,
            identity_id := .identity.id
        }
        filter .id = <uuid>$id
        and (datetime_current() - .created_at) < <duration>'10 minutes';
        """,
        variables={"id": id},
    )

    result_json = json.loads(r.decode())
    assert len(result_json) == 1

    return PKCEChallenge(**result_json[0])


async def delete(db, id: str) -> None:
    r = await execute.parse_execute_json(
        db,
        """
        delete ext::auth::PKCEChallenge filter .id = <uuid>$id
        """,
        variables={"id": id},
    )

    result_json = json.loads(r.decode())
    assert len(result_json) == 1
