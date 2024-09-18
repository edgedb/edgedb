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
from http.cookiejar import CookieJar

import dataclasses
import json
import typing
import asyncio
import logging
import httpx

from edb.ir import statypes
from edb.server import defines
from edb.server.protocol import execute
from . import dbview

if typing.TYPE_CHECKING:
    from edb.server import server as edbserver
    from edb.server import tenant as edbtenant


logger = logging.getLogger("edb.server")

POLLING_INTERVAL = statypes.Duration(microseconds=10 * 1_000_000)  # 10 seconds
# TODO: Replace with config value
MAX_CONCURRENT_CONNECTIONS = 10
LIMITS = httpx.Limits(max_connections=MAX_CONCURRENT_CONNECTIONS)


class NullCookieJar(CookieJar):
    """A CookieJar that rejects all cookies."""

    def extract_cookies(self, *_):
        pass

    def set_cookie(self, _):
        pass


async def _http(tenant: edbtenant.Tenant) -> None:
    try:
        async with (
            httpx.AsyncClient(
                limits=LIMITS,
                cookies=NullCookieJar(),
            ) as client,
            asyncio.TaskGroup() as g,
        ):
            for db in tenant.iter_dbs():
                if db.name == defines.EDGEDB_SYSTEM_DB:
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
                    g.create_task(handle_request(client, db, request))
    except Exception as ex:
        logger.debug(
            "HTTP send failed (instance: %s)",
            tenant.get_instance_name(),
            exc_info=ex,
        )


async def http(server: edbserver.BaseServer) -> None:
    while True:
        try:
            tasks = [
                tenant.create_task(_http(tenant), interruptable=False)
                for tenant in server.iter_tenants()
                if tenant.accept_new_tasks
            ]
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


async def handle_request(
    client: httpx.AsyncClient, db: dbview.Database, request: ScheduledRequest
) -> None:
    try:
        headers = (
            [(header["name"], header["value"]) for header in request.headers]
            if request.headers
            else None
        )
        response = await client.request(
            request.method,
            request.url,
            content=request.body,
            headers=headers,
        )
        request_state = 'Completed'
        failure = None
        response_status = response.status_code
        response_headers = list(response.headers.items())
    except Exception as ex:
        response = None
        request_state = 'Failed'
        failure = {
            'kind': 'NetworkError',
            'message': str(ex),
        }
        response_status = None
        response_status = None
        response_body = None
        response_headers = None

    if response is not None:
        try:
            response_body = await response.aread()
        except Exception as ex:
            logger.debug("Failed to read response body", exc_info=ex)
            response_body = None
    else:
        response_body = None

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
            FOUND := <nh::ScheduledRequest><uuid>$id,
        update FOUND
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
