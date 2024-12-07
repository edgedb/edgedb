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

from __future__ import annotations
import typing

import asyncio
import json
import logging
import dataclasses

from edb.ir import statypes
from edb.server.protocol import execute

if typing.TYPE_CHECKING:
    from edb.server import server as edbserver
    from edb.server import tenant as edbtenant


logger = logging.getLogger("edb.server")
VALIDITY = statypes.Duration.from_microseconds(10 * 60_000_000)  # 10 minutes


@dataclasses.dataclass(repr=False)
class PKCEChallenge:
    """
    Object that represents the ext::auth::PKCEChallenge type
    """

    id: str
    challenge: str
    auth_token: str | None
    refresh_token: str | None
    id_token: str | None
    identity_id: str | None


async def create(db: edbtenant.dbview.Database, challenge: str) -> None:
    await execute.parse_execute_json(
        db,
        """
        insert ext::auth::PKCEChallenge {
            challenge := <str>$challenge,
        } unless conflict on .challenge
        else (select ext::auth::PKCEChallenge)
        """,
        variables={
            "challenge": challenge,
        },
        cached_globally=True,
        query_tag='gel/auth',
    )


async def link_identity_challenge(
    db: edbtenant.dbview.Database,
    identity_id: str,
    challenge: str,
) -> str:
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
        cached_globally=True,
        query_tag='gel/auth',
    )

    result_json = json.loads(r.decode())
    assert len(result_json) == 1

    return typing.cast(str, result_json[0]["id"])


async def add_provider_tokens(
    db: edbtenant.dbview.Database,
    id: str,
    auth_token: str | None,
    refresh_token: str | None,
    id_token: str | None,
) -> str:
    r = await execute.parse_execute_json(
        db,
        """
        update ext::auth::PKCEChallenge
        filter .id = <uuid>$id
        set {
            auth_token := <optional str>$auth_token,
            refresh_token := <optional str>$refresh_token,
            id_token := <optional str>$id_token,
        }
        """,
        variables={
            "id": id,
            "auth_token": auth_token,
            "refresh_token": refresh_token,
            "id_token": id_token,
        },
        cached_globally=True,
        query_tag='gel/auth',
    )

    result_json = json.loads(r.decode())
    assert len(result_json) == 1

    return typing.cast(str, result_json[0]["id"])


async def get_by_id(db: edbtenant.dbview.Database, id: str) -> PKCEChallenge:
    r = await execute.parse_execute_json(
        db,
        """
        select ext::auth::PKCEChallenge {
            id,
            challenge,
            auth_token,
            refresh_token,
            id_token,
            identity_id := .identity.id
        }
        filter .id = <uuid>$id
        and (datetime_current() - .created_at) < <duration>$validity;
        """,
        variables={"id": id, "validity": VALIDITY.to_backend_str()},
        cached_globally=True,
        query_tag='gel/auth',
    )

    result_json = json.loads(r.decode())
    assert len(result_json) == 1

    return PKCEChallenge(**result_json[0])


async def delete(db: edbtenant.dbview.Database, id: str) -> None:
    r = await execute.parse_execute_json(
        db,
        """
        delete ext::auth::PKCEChallenge filter .id = <uuid>$id
        """,
        variables={"id": id},
        cached_globally=True,
        query_tag='gel/auth',
    )

    result_json = json.loads(r.decode())
    assert len(result_json) == 1


async def _delete_challenge(db: edbtenant.dbview.Database) -> None:
    if not db.tenant.is_database_connectable(db.name):
        # Don't run gc if the database is not connectable, e.g. being dropped
        return

    await execute.parse_execute_json(
        db,
        """
        delete ext::auth::PKCEChallenge filter
            (datetime_of_statement() - .created_at) >
            <duration>$validity
        """,
        variables={"validity": VALIDITY.to_backend_str()},
        cached_globally=True,
        query_tag='gel/auth',
    )


async def _gc(tenant: edbtenant.Tenant) -> None:
    try:
        async with asyncio.TaskGroup() as g:
            for db in tenant.iter_dbs():
                if "auth" in db.extensions:
                    g.create_task(_delete_challenge(db))
    except Exception as ex:
        logger.debug(
            "GC of ext::auth::PKCEChallenge failed (instance: %s)",
            tenant.get_instance_name(),
            exc_info=ex,
        )


async def gc(server: edbserver.BaseServer) -> None:
    while True:
        try:
            tasks = [
                tenant.create_task(_gc(tenant), interruptable=True)
                for tenant in server.iter_tenants()
                if tenant.accept_new_tasks
            ]
            if tasks:
                await asyncio.wait(tasks)
        except Exception as ex:
            logger.debug("GC of ext::auth::PKCEChallenge failed", exc_info=ex)
        finally:
            await asyncio.sleep(VALIDITY.to_microseconds() / 1_000_000.0)
