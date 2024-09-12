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
from enum import Enum

import dataclasses
import json
import typing

import asyncio
import logging

import httpx

from edb.ir import statypes
from edb.server.protocol import execute
from . import dbview

if typing.TYPE_CHECKING:
    from edb.server import server as edbserver
    from edb.server import tenant as edbtenant


logger = logging.getLogger("edb.server")
POLLING_INTERVAL = statypes.Duration(microseconds=10 * 1_000_000)  # 10 seconds


async def _http(tenant: edbtenant.Tenant) -> None:
    try:
        async with asyncio.TaskGroup() as g:
            for db in tenant.iter_dbs():
                json_bytes = await execute.parse_execute_json(
                    db,
                    """
                    with
                        PENDING_REQUESTS := (
                            select std::net::http::ScheduledRequest
                            filter .status = std::net::RequestState.Pending
                        ),
                        UPDATED := (
                            update PENDING_REQUESTS
                            set {
                                status := std::net::RequestState.InProgress,
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
                    g.create_task(handle_request(db, request))
                    request = ScheduledRequest(**pending_request)
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


class HTTPMethod(str, Enum):
    GET = 'GET'
    POST = 'POST'
    PUT = 'PUT'
    DELETE = 'DELETE'
    HEAD = 'HEAD'
    OPTIONS = 'OPTIONS'
    PATCH = 'PATCH'


@dataclasses.dataclass
class ScheduledRequest:
    id: str
    method: HTTPMethod
    url: str
    body: typing.Optional[bytes]
    headers: typing.Optional[list[tuple[str, str]]]


async def http_send(request: ScheduledRequest) -> httpx.Response:
    async with httpx.AsyncClient() as client:
        response = await client.request(
            request.method,
            request.url,
            content=request.body,
            headers=request.headers,
        )
        return response


async def handle_request(
    db: dbview.Database, request: ScheduledRequest
) -> None:
    try:
        response = await http_send(request)
        request_state = 'Completed'
        response_status = response.status_code
        response_body = await response.aread()
        response_headers = list(response.headers.items())
    except Exception:
        request_state = 'Failed'
        response_status = None
        response_body = None
        response_headers = None

    json_bytes = await execute.parse_execute_json(
        db,
        """
        with
            nh as module std::net::http,
            net as module std::net,
            state := <net::RequestState>$state,
            response_status := <optional int16>$response_status,
            response_body := <optional bytes>$response_body,
            response_headers :=
                <optional set<tuple<str, str>>>$response_headers,
            response := (
                if state = net::RequestState.Completed
                then (
                    insert nh::Response {
                        status := assert_exists(response_status),
                        body := response_body,
                        headers := response_headers,
                    }
                )
                else (<nh::Response>{})
            )
        update nh::ScheduledRequest
        set {
            state := state,
            response := response,
        }
        """,
        variables={
            'state': request_state,
            'response_status': response_status,
            'response_body': response_body,
            'response_headers': response_headers,
        },
        cached_globally=True,
    )
