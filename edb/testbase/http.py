#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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
from typing import (
    Any,
    Callable,
    Optional,
    Type,
)

import http.server
import json
import threading
import urllib.parse
import urllib.request
import dataclasses

import edgedb

from edb.errors import base as base_errors

from edb.common import assert_data_shape

from . import server


bag = assert_data_shape.bag


class BaseHttpTest(server.QueryTestCase):
    @classmethod
    async def _wait_for_db_config(
        cls,
        config_key,
        *,
        server=None,
        instance_config=False,
        value=None,
        is_reset=False,
    ):
        dbname = cls.get_database_name()
        # Wait for the database config changes to propagate to the
        # server by watching a debug endpoint
        async for tr in cls.try_until_succeeds(
            ignore=AssertionError,
            timeout=120,
        ):
            async with tr:
                with cls.http_con(server) as http_con:
                    (
                        rdata,
                        _headers,
                        _status,
                    ) = cls.http_con_request(
                        http_con,
                        prefix="",
                        path="server-info",
                    )
                    data = json.loads(rdata)
                    if "databases" not in data:
                        # multi-tenant instance - use the first tenant
                        data = next(iter(data["tenants"].values()))
                    if instance_config:
                        config = data["instance_config"]
                    else:
                        config = data["databases"][dbname]["config"]
                    if is_reset:
                        if config_key in config:
                            raise AssertionError("database config not ready")
                    else:
                        if config_key not in config:
                            raise AssertionError("database config not ready")
                        if value and config[config_key] != value:
                            raise AssertionError("database config not ready")


class BaseHttpExtensionTest(BaseHttpTest):
    @classmethod
    def get_extension_path(cls):
        raise NotImplementedError

    @classmethod
    def get_api_prefix(cls):
        extpath = cls.get_extension_path()
        dbname = cls.get_database_name()
        return f"/branch/{dbname}/{extpath}"


class ExtAuthTestCase(BaseHttpExtensionTest):
    EXTENSIONS = ["pgcrypto", "auth"]

    @classmethod
    def get_extension_path(cls):
        return "ext/auth"


class EdgeQLTestCase(BaseHttpExtensionTest):
    EXTENSIONS = ["edgeql_http"]

    @classmethod
    def get_extension_path(cls):
        return "edgeql"

    def edgeql_query(
        self,
        query,
        *,
        use_http_post=True,
        variables=None,
        globals=None,
        origin=None,
    ):
        req_data = {"query": query}

        if use_http_post:
            if variables is not None:
                req_data["variables"] = variables
            if globals is not None:
                req_data["globals"] = globals
            req = urllib.request.Request(self.http_addr, method="POST")
            req.add_header("Content-Type", "application/json")
            req.add_header("Authorization", self.make_auth_header())
            if origin:
                req.add_header("Origin", origin)
            response = urllib.request.urlopen(
                req, json.dumps(req_data).encode(), context=self.tls_context
            )
            resp_data = json.loads(response.read())
        else:
            if variables is not None:
                req_data["variables"] = json.dumps(variables)
            if globals is not None:
                req_data["globals"] = json.dumps(globals)
            req = urllib.request.Request(
                f"{self.http_addr}/?{urllib.parse.urlencode(req_data)}",
            )
            req.add_header("Authorization", self.make_auth_header())
            response = urllib.request.urlopen(
                req,
                context=self.tls_context,
            )
            resp_data = json.loads(response.read())

        if "data" in resp_data:
            return (resp_data["data"], response)

        err = resp_data["error"]

        ex_msg = err["message"].strip()
        ex_code = err["code"]

        raise edgedb.EdgeDBError._from_code(ex_code, ex_msg)

    def assert_edgeql_query_result(
        self,
        query,
        result,
        *,
        msg=None,
        sort=None,
        use_http_post=True,
        variables=None,
        globals=None,
    ):
        res, _ = self.edgeql_query(
            query,
            use_http_post=use_http_post,
            variables=variables,
            globals=globals,
        )

        if sort is not None:
            # GQL will always have a single object returned. The data is
            # in the top-level fields, so that's what needs to be sorted.
            for r in res.values():
                assert_data_shape.sort_results(r, sort)

        assert_data_shape.assert_data_shape(res, result, self.fail, message=msg)
        return res


class GraphQLTestCase(BaseHttpExtensionTest):
    EXTENSIONS = ["graphql"]

    @classmethod
    def get_extension_path(cls):
        return "graphql"

    def graphql_query(
        self,
        query,
        *,
        operation_name=None,
        use_http_post=True,
        variables=None,
        globals=None,
        deprecated_globals=None,
    ):
        req_data = {"query": query}

        if operation_name is not None:
            req_data["operationName"] = operation_name

        if use_http_post:
            if variables is not None:
                req_data["variables"] = variables
            if globals is not None:
                if variables is None:
                    req_data["variables"] = dict()
                req_data["variables"]["__globals__"] = globals
            # Support testing the old way of sending globals.
            if deprecated_globals is not None:
                req_data["globals"] = deprecated_globals

            req = urllib.request.Request(self.http_addr, method="POST")
            req.add_header("Content-Type", "application/json")
            req.add_header("Authorization", self.make_auth_header())
            response = urllib.request.urlopen(
                req, json.dumps(req_data).encode(), context=self.tls_context
            )
            resp_data = json.loads(response.read())
        else:
            if globals is not None:
                if variables is None:
                    variables = dict()
                variables["__globals__"] = globals
            # Support testing the old way of sending globals.
            if deprecated_globals is not None:
                req_data["globals"] = json.dumps(deprecated_globals)
            if variables is not None:
                req_data["variables"] = json.dumps(variables)
            req = urllib.request.Request(
                f"{self.http_addr}/?{urllib.parse.urlencode(req_data)}",
            )
            req.add_header("Authorization", self.make_auth_header())
            response = urllib.request.urlopen(
                req,
                context=self.tls_context,
            )
            resp_data = json.loads(response.read())

        if "data" in resp_data:
            return resp_data["data"]

        err = resp_data["errors"][0]

        typename, msg = err["message"].split(":", 1)
        msg = msg.strip()

        try:
            ex_type = getattr(edgedb, typename)
        except AttributeError:
            raise AssertionError(
                f"server returned an invalid exception typename: {typename!r}"
                f"\n  Message: {msg}"
            )

        ex = ex_type(msg)

        if "locations" in err:
            # XXX Fix this when LSP "location" objects are implemented
            ex._attrs[base_errors.FIELD_LINE_START] = str(
                err["locations"][0]["line"]
            ).encode()
            ex._attrs[base_errors.FIELD_COLUMN_START] = str(
                err["locations"][0]["column"]
            ).encode()

        raise ex

    def assert_graphql_query_result(
        self,
        query,
        result,
        *,
        msg=None,
        sort=None,
        operation_name=None,
        use_http_post=True,
        variables=None,
        globals=None,
        deprecated_globals=None,
    ):
        res = self.graphql_query(
            query,
            operation_name=operation_name,
            use_http_post=use_http_post,
            variables=variables,
            globals=globals,
            deprecated_globals=deprecated_globals,
        )

        if sort is not None:
            # GQL will always have a single object returned. The data is
            # in the top-level fields, so that's what needs to be sorted.
            for r in res.values():
                assert_data_shape.sort_results(r, sort)

        assert_data_shape.assert_data_shape(res, result, self.fail, message=msg)
        return res


class MockHttpServerHandler(http.server.BaseHTTPRequestHandler):
    def get_server_and_path(self) -> tuple[str, str]:
        server = f'http://{self.headers.get("Host")}'
        return server, self.path

    def do_GET(self):
        self.close_connection = False
        server, path = self.get_server_and_path()
        self.server.owner.handle_request("GET", server, path, self)

    def do_POST(self):
        self.close_connection = False
        server, path = self.get_server_and_path()
        self.server.owner.handle_request("POST", server, path, self)

    def log_message(self, *args):
        pass


class MultiHostMockHttpServerHandler(MockHttpServerHandler):
    def get_server_and_path(self) -> tuple[str, str]:
        # Path looks like:
        # http://127.0.0.1:32881/https%3A//slack.com/.well-known/openid-configuration
        raw_url = urllib.parse.unquote(self.path.lstrip("/"))
        url = urllib.parse.urlparse(raw_url)
        return (f"{url.scheme}://{url.netloc}", url.path.lstrip("/"))


ResponseType = tuple[str, int] | tuple[str, int, dict[str, str]]


@dataclasses.dataclass
class RequestDetails:
    headers: dict[str, str | Any]
    query_params: dict[str, list[str]]
    body: Optional[str]


class MockHttpServer:
    def __init__(
        self,
        handler_type: Type[MockHttpServerHandler] = MockHttpServerHandler,
    ) -> None:
        self.has_started = threading.Event()
        self.routes: dict[
            tuple[str, str, str],
            (
                ResponseType
                | Callable[
                    [MockHttpServerHandler, RequestDetails], ResponseType
                ]
            ),
        ] = {}
        self.requests: dict[tuple[str, str, str], list[RequestDetails]] = {}
        self.url: Optional[str] = None
        self.handler_type = handler_type

    def get_base_url(self) -> str:
        if self.url is None:
            raise RuntimeError("mock server is not running")
        return self.url

    def register_route_handler(
        self,
        method: str,
        server: str,
        path: str,
    ):
        def wrapper(
            handler: (
                ResponseType
                | Callable[
                    [MockHttpServerHandler, RequestDetails], ResponseType
                ]
            ),
        ):
            self.routes[(method, server, path)] = handler
            return handler

        return wrapper

    def handle_request(
        self,
        method: str,
        server: str,
        path: str,
        handler: MockHttpServerHandler,
    ):
        # `handler` is documented here:
        # https://docs.python.org/3/library/http.server.html#http.server.BaseHTTPRequestHandler
        key = (method, server, path)
        if key not in self.requests:
            self.requests[key] = []

        # Parse and save the request details
        parsed_path = urllib.parse.urlparse(path)
        headers = {k.lower(): v for k, v in dict(handler.headers).items()}
        query_params = urllib.parse.parse_qs(parsed_path.query)
        if "content-length" in headers:
            body = handler.rfile.read(int(headers["content-length"])).decode()
        else:
            body = None

        request_details = RequestDetails(
            headers=headers,
            query_params=query_params,
            body=body,
        )
        self.requests[key].append(request_details)
        if key not in self.routes:
            error_message = (
                f"No route handler for {key}\n\n"
                f"Available routes:\n{self.routes}"
            )
            handler.send_error(404, message=error_message)
            return

        registered_handler = self.routes[key]

        if callable(registered_handler):
            try:
                handler_result = registered_handler(handler, request_details)
                if len(handler_result) == 2:
                    response, status = handler_result
                    additional_headers = None
                elif len(handler_result) == 3:
                    response, status, additional_headers = handler_result
            except Exception:
                handler.send_error(500)
                raise
        else:
            if len(registered_handler) == 2:
                response, status = registered_handler
                additional_headers = None
            elif len(registered_handler) == 3:
                response, status, additional_headers = registered_handler

        accept_header = request_details.headers.get(
            "accept", "application/json"
        )

        if (
            accept_header.startswith("application/json")
            or (
                accept_header.startswith("application/")
                and "vnd." in accept_header
                and "+json" in accept_header
            )
            or accept_header == "*/*"
        ):
            content_type = "application/json"
        elif accept_header.startswith("application/x-www-form-urlencoded"):
            content_type = "application/x-www-form-urlencoded"
        else:
            handler.send_error(
                415, f"Unsupported accept header: {accept_header}"
            )
            return

        data = response.encode()

        handler.send_response(status)
        handler.send_header("Content-Type", content_type)
        handler.send_header("Content-Length", str(len(data)))
        if additional_headers is not None:
            for header, value in additional_headers.items():
                handler.send_header(header, value)
        handler.end_headers()
        handler.wfile.write(data)

    def start(self):
        assert not hasattr(self, "_http_runner")
        self._http_runner = threading.Thread(target=self._http_worker)
        self._http_runner.start()
        self.has_started.wait()
        self.url = f"http://{self._address[0]}:{self._address[1]}/"

    def __enter__(self):
        self.start()
        return self

    def _http_worker(self):
        self._http_server = http.server.HTTPServer(
            ("localhost", 0), self.handler_type
        )
        self._http_server.owner = self
        self._address = self._http_server.server_address
        self.has_started.set()
        self._http_server.serve_forever(poll_interval=0.01)
        self._http_server.server_close()

    def stop(self):
        self._http_server.shutdown()
        if self._http_runner is not None:
            self._http_runner.join(timeout=60)
            if self._http_runner.is_alive():
                raise RuntimeError("Mock HTTP server failed to stop")
            self._http_runner = None

    def __exit__(self, *exc):
        self.stop()
        self.url = None
