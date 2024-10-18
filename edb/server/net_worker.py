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
import os

from edb.ir import statypes
from edb.server import defines
from edb.server.protocol import execute
from edb.server._http import Http
from edb.common import retryloop
from . import dbview

if typing.TYPE_CHECKING:
    from edb.server import server as edbserver
    from edb.server import tenant as edbtenant

logger = logging.getLogger("edb.server.net_worker")

POLLING_INTERVAL = statypes.Duration(microseconds=10 * 1_000_000)  # 10 seconds
# TODO: Make this configurable via server config
NET_HTTP_REQUEST_TTL = statypes.Duration(
    microseconds=3600 * 1_000_000
)  # 1 hour


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
                                updated_at := datetime_of_statement(),
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
                    tx_isolation=defines.TxIsolationLevel.RepeatableRead,
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


class HttpClient:
    def __init__(self, limit: int):
        self._client = Http(limit)
        self._fd = self._client._fd
        self._task = None
        self._skip_reads = 0
        self._loop = asyncio.get_running_loop()
        self._task = self._loop.create_task(self._boot(self._loop))
        self._next_id = 0
        self._requests: dict[int, asyncio.Future] = {}

    def __del__(self) -> None:
        if self._task:
            self._task.cancel()
            self._task = None

    def _update_limit(self, limit: int):
        self._client._update_limit(limit)

    async def request(
        self,
        *,
        method: str,
        url: str,
        content: bytes | None,
        headers: list[tuple[str, str]] | None,
    ):
        if content is None:
            content = bytes()
        if headers is None:
            headers = []
        id = self._next_id
        self._next_id += 1
        self._requests[id] = asyncio.Future()
        try:
            self._client._request(id, url, method, content, headers)
            resp = await self._requests[id]
            return resp
        finally:
            del self._requests[id]

    async def _boot(self, loop: asyncio.AbstractEventLoop) -> None:
        logger.info("Python-side HTTP client booted")
        reader = asyncio.StreamReader(loop=loop)
        reader_protocol = asyncio.StreamReaderProtocol(reader)
        fd = os.fdopen(self._client._fd, 'rb')
        transport, _ = await loop.connect_read_pipe(lambda: reader_protocol, fd)
        try:
            while len(await reader.read(1)) == 1:
                if not self._client or not self._task:
                    break
                if self._skip_reads > 0:
                    self._skip_reads -= 1
                    continue
                msg = self._client._read()
                if not msg:
                    break
                self._process_message(msg)
        finally:
            transport.close()

    def _process_message(self, msg):
        msg_type, id, data = msg

        if id in self._requests:
            if msg_type == 1:
                self._requests[id].set_result(data)
            elif msg_type == 0:
                self._requests[id].set_exception(Exception(data))


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
                        updated_at := datetime_of_statement(),
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
                    tx_isolation=defines.TxIsolationLevel.RepeatableRead,
                )

    await _update_request()


async def _delete_requests(
    db: dbview.Database, expires_in: statypes.Duration
) -> None:
    def _warn(e):
        logger.warning(
            "Failed to delete std::net::http::ScheduledRequest, retrying."
            " Reason: %s",
            e,
        )

    rloop = retryloop.RetryLoop(
        backoff=retryloop.exp_backoff(),
        timeout=300,
        ignore=(Exception,),
        retry_cb=_warn,
    )
    async for iteration in rloop:
        async with iteration:
            result = await execute.parse_execute_json(
                db,
                """
                with requests := (
                    select std::net::http::ScheduledRequest filter
                    .state != std::net::RequestState.Pending
                    and (datetime_of_statement() - .updated_at) >
                    <duration>$expires_in
                )
                delete requests;
                """,
                variables={"expires_in": expires_in.to_backend_str()},
                cached_globally=True,
            )
            if len(result) > 0:
                logger.info(f"Deleted requests: {result!r}")
            else:
                logger.info(f"No requests to delete")


async def _gc(tenant: edbtenant.Tenant, expires_in: statypes.Duration) -> None:
    try:
        async with asyncio.TaskGroup() as g:
            for db in tenant.iter_dbs():
                if db.name == defines.EDGEDB_SYSTEM_DB:
                    continue
                g.create_task(_delete_requests(db, expires_in))
    except Exception as ex:
        logger.debug(
            "GC of std::net::http::ScheduledRequest failed (instance: %s)",
            tenant.get_instance_name(),
            exc_info=ex,
        )


async def gc(server: edbserver.BaseServer) -> None:
    while True:
        tasks = [
            tenant.create_task(
                _gc(tenant, NET_HTTP_REQUEST_TTL), interruptable=False
            )
            for tenant in server.iter_tenants()
            if tenant.accept_new_tasks
        ]
        try:
            await asyncio.wait(tasks)
        except Exception as ex:
            logger.debug(
                "GC of std::net::http::ScheduledRequest failed", exc_info=ex
            )
        finally:
            await asyncio.sleep(
                NET_HTTP_REQUEST_TTL.to_microseconds() / 1_000_000.0
            )
