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
import json
import typing
import asyncio
import logging
import base64

from edb.ir import statypes
from edb.server import defines
from edb.server.protocol import execute
from edb.server.http import HttpClient
from edb.common import retryloop
from . import dbview

if typing.TYPE_CHECKING:
    from edb.server import server as edbserver
    from edb.server import tenant as edbtenant

logger = logging.getLogger("edb.server")

POLLING_INTERVAL = statypes.Duration(microseconds=10 * 1_000_000)  # 10 seconds


async def _http_task(tenant: edbtenant.Tenant, http_client) -> None:
    net_http_max_connections = tenant._server.config_lookup(
        'net_http_max_connections', tenant.get_sys_config()
    )
    http_client._update_limit(net_http_max_connections)
    try:
        async with (asyncio.TaskGroup() as g,):
            for db in tenant.iter_dbs():
                if db.name == defines.EDGEDB_SYSTEM_DB:
                    # Don't run the net_worker for system database
                    continue
                if not tenant.is_database_connectable(db.name):
                    # Don't run the net_worker if the database is not
                    # connectable, e.g. being dropped
                    continue
                json_bytes = await execute.parse_execute_json(
                    db,
                    """
                    with
                        PENDING_REQUESTS := (
                            select std::net::http::ScheduledRequest
                            filter .state = std::net::RequestState.Pending
                        ),
                        UPDATED := (
                            update PENDING_REQUESTS
                            set {
                                state := std::net::RequestState.InProgress,
                            }
                        ),
                    select UPDATED {
                        id,
                        method,
                        url,
                        body,
                        headers,
                    }
                    """,
                    cached_globally=True,
                )
                pending_requests: list[dict] = json.loads(json_bytes)
                for pending_request in pending_requests:
                    request = ScheduledRequest(**pending_request)
                    g.create_task(handle_request(http_client, db, request))
    except Exception as ex:
        logger.debug(
            "HTTP send failed (instance: %s)",
            tenant.get_instance_name(),
            exc_info=ex,
        )


def create_http(tenant: edbtenant.Tenant):
    net_http_max_connections = tenant._server.config_lookup(
        'net_http_max_connections', tenant.get_sys_config()
    )
    return HttpClient(net_http_max_connections)


async def http(server: edbserver.BaseServer) -> None:
    tenant_http = dict()

    while True:
        tenant_set = set()
        try:
            tasks = []
            for tenant in server.iter_tenants():
                if tenant.accept_new_tasks:
                    tenant_set.add(tenant)
                    if tenant not in tenant_http:
                        tenant_http[tenant] = create_http(tenant)
                    tasks.append(
                        tenant.create_task(
                            _http_task(tenant, tenant_http[tenant]),
                            interruptable=False,
                        )
                    )
            # Remove unused tenant_http entries
            for tenant in tenant_http.keys():
                if tenant not in tenant_set:
                    del tenant_http[tenant]
            if tasks:
                await asyncio.wait(tasks)
        except Exception as ex:
            logger.debug("HTTP worker failed", exc_info=ex)
        finally:
            await asyncio.sleep(
                POLLING_INTERVAL.to_microseconds() / 1_000_000.0
            )


@dataclasses.dataclass
class ScheduledRequest:
    id: str
    method: str
    url: str
    body: typing.Optional[bytes]
    headers: typing.Optional[list[dict]]

    def __post_init__(self):
        if self.body is not None:
            self.body = base64.b64decode(self.body).decode('utf-8').encode()


async def handle_request(
    client: HttpClient, db: dbview.Database, request: ScheduledRequest
) -> None:
    response_status = None
    response_body = None
    response_headers = None
    failure = None

    try:
        headers = (
            [(header["name"], header["value"]) for header in request.headers]
            if request.headers
            else None
        )
        response = await client.request(
            method=request.method,
            url=request.url,
            content=request.body,
            headers=headers,
        )
        response_status, response_bytes, response_hdict = response
        response_body = bytes(response_bytes)
        response_headers = list(response_hdict.items())
        request_state = 'Completed'
    except Exception as ex:
        request_state = 'Failed'
        failure = {
            'kind': 'NetworkError',
            'message': str(ex),
        }

    def _warn(e):
        logger.warning(
            "Failed to update std::net::http record, retrying. Reason: %s", e
        )

    async def _update_request():
        rloop = retryloop.RetryLoop(
            backoff=retryloop.exp_backoff(),
            timeout=300,
            ignore=(Exception,),
            retry_cb=_warn,
        )
        async for iteration in rloop:
            async with iteration:
                await execute.parse_execute_json(
                    db,
                    """
                    with
                        nh as module std::net::http,
                        net as module std::net,
                        state := <net::RequestState>$state,
                        failure := <
                            optional tuple<
                                kind: net::RequestFailureKind,
                                message: str
                            >
                        >to_json(<str>$failure),
                        response_status := <optional int16>$response_status,
                        response_body := <optional bytes>$response_body,
                        response_headers :=
                            <optional array<tuple<str, str>>>$response_headers,
                        response := (
                            if state = net::RequestState.Completed
                            then (
                                insert nh::Response {
                                    created_at := datetime_of_statement(),
                                    status := assert_exists(response_status),
                                    body := response_body,
                                    headers := response_headers,
                                }
                            )
                            else (<nh::Response>{})
                        ),
                    update nh::ScheduledRequest filter .id = <uuid>$id
                    set {
                        state := state,
                        response := response,
                        failure := failure,
                    };
                    """,
                    variables={
                        'id': request.id,
                        'state': request_state,
                        'response_status': response_status,
                        'response_body': response_body,
                        'response_headers': response_headers,
                        'failure': json.dumps(failure),
                    },
                    cached_globally=True,
                )

    await _update_request()
