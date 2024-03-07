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

import json
import urllib.parse
import urllib.request

import edgedb

from edb.errors import base as base_errors

from edb.common import assert_data_shape

from . import server


bag = assert_data_shape.bag


class BaseHttpExtensionTest(server.QueryTestCase):
    @classmethod
    def get_extension_path(cls):
        raise NotImplementedError

    @classmethod
    def get_api_prefix(cls):
        extpath = cls.get_extension_path()
        dbname = cls.get_database_name()
        return f'/branch/{dbname}/{extpath}'

    @classmethod
    def tearDownClass(cls):
        # This isn't really necessary but helps test extension dropping
        for extname in reversed(cls.EXTENSIONS):
            cls.loop.run_until_complete(
                cls.con.execute(f'DROP EXTENSION {extname};')
            )
        super().tearDownClass()

    @classmethod
    async def _wait_for_db_config(
        cls, config_key, *, server=None, instance_config=False, value=None
    ):
        dbname = cls.get_database_name()
        # Wait for the database config changes to propagate to the
        # server by watching a debug endpoint
        async for tr in cls.try_until_succeeds(ignore=AssertionError):
            async with tr:
                with cls.http_con(server) as http_con:
                    (
                        rdata,
                        _headers,
                        status,
                    ) = cls.http_con_request(
                        http_con,
                        prefix="",
                        path="server-info",
                    )
                    data = json.loads(rdata)
                    if 'databases' not in data:
                        # multi-tenant instance - use the first tenant
                        data = next(iter(data['tenants'].values()))
                    if instance_config:
                        config = data['instance_config']
                    else:
                        config = data['databases'][dbname]['config']
                    if config_key not in config:
                        raise AssertionError('database config not ready')
                    if value and config[config_key] != value:
                        raise AssertionError(f'database config not ready')


class ExtAuthTestCase(BaseHttpExtensionTest):

    EXTENSIONS = ['pgcrypto', 'auth']

    @classmethod
    def get_extension_path(cls):
        return 'ext/auth'


class EdgeQLTestCase(BaseHttpExtensionTest):

    EXTENSIONS = ['edgeql_http']

    @classmethod
    def get_extension_path(cls):
        return 'edgeql'

    def edgeql_query(
            self,
            query,
            *,
            use_http_post=True,
            variables=None,
            globals=None,
            origin=None,
    ):
        req_data = {
            'query': query
        }

        if use_http_post:
            if variables is not None:
                req_data['variables'] = variables
            if globals is not None:
                req_data['globals'] = globals
            req = urllib.request.Request(self.http_addr, method='POST')
            req.add_header('Content-Type', 'application/json')
            req.add_header('Authorization', self.make_auth_header())
            if origin:
                req.add_header('Origin', origin)
            response = urllib.request.urlopen(
                req, json.dumps(req_data).encode(), context=self.tls_context
            )
            resp_data = json.loads(response.read())
        else:
            if variables is not None:
                req_data['variables'] = json.dumps(variables)
            if globals is not None:
                req_data['globals'] = json.dumps(globals)
            req = urllib.request.Request(
                f'{self.http_addr}/?{urllib.parse.urlencode(req_data)}',
            )
            req.add_header('Authorization', self.make_auth_header())
            response = urllib.request.urlopen(
                req,
                context=self.tls_context,
            )
            resp_data = json.loads(response.read())

        if 'data' in resp_data:
            return (resp_data['data'], response)

        err = resp_data['error']

        ex_msg = err['message'].strip()
        ex_code = err['code']

        raise edgedb.EdgeDBError._from_code(ex_code, ex_msg)

    def assert_edgeql_query_result(self, query, result, *,
                                   msg=None, sort=None,
                                   use_http_post=True,
                                   variables=None,
                                   globals=None):
        res, _ = self.edgeql_query(
            query,
            use_http_post=use_http_post,
            variables=variables,
            globals=globals)

        if sort is not None:
            # GQL will always have a single object returned. The data is
            # in the top-level fields, so that's what needs to be sorted.
            for r in res.values():
                assert_data_shape.sort_results(r, sort)

        assert_data_shape.assert_data_shape(
            res, result, self.fail, message=msg)
        return res


class GraphQLTestCase(BaseHttpExtensionTest):

    EXTENSIONS = ['graphql']

    @classmethod
    def get_extension_path(cls):
        return 'graphql'

    def graphql_query(self, query, *, operation_name=None,
                      use_http_post=True,
                      variables=None,
                      globals=None,
                      deprecated_globals=None):
        req_data = {
            'query': query
        }

        if operation_name is not None:
            req_data['operationName'] = operation_name

        if use_http_post:
            if variables is not None:
                req_data['variables'] = variables
            if globals is not None:
                if variables is None:
                    req_data['variables'] = dict()
                req_data['variables']['__globals__'] = globals
            # Support testing the old way of sending globals.
            if deprecated_globals is not None:
                req_data['globals'] = deprecated_globals

            req = urllib.request.Request(self.http_addr, method='POST')
            req.add_header('Content-Type', 'application/json')
            req.add_header('Authorization', self.make_auth_header())
            response = urllib.request.urlopen(
                req, json.dumps(req_data).encode(), context=self.tls_context
            )
            resp_data = json.loads(response.read())
        else:
            if globals is not None:
                if variables is None:
                    variables = dict()
                variables['__globals__'] = globals
            # Support testing the old way of sending globals.
            if deprecated_globals is not None:
                req_data['globals'] = json.dumps(deprecated_globals)
            if variables is not None:
                req_data['variables'] = json.dumps(variables)
            req = urllib.request.Request(
                f'{self.http_addr}/?{urllib.parse.urlencode(req_data)}',
            )
            req.add_header('Authorization', self.make_auth_header())
            response = urllib.request.urlopen(
                req,
                context=self.tls_context,
            )
            resp_data = json.loads(response.read())

        if 'data' in resp_data:
            return resp_data['data']

        err = resp_data['errors'][0]

        typename, msg = err['message'].split(':', 1)
        msg = msg.strip()

        try:
            ex_type = getattr(edgedb, typename)
        except AttributeError:
            raise AssertionError(
                f'server returned an invalid exception typename: {typename!r}'
                f'\n  Message: {msg}')

        ex = ex_type(msg)

        if 'locations' in err:
            # XXX Fix this when LSP "location" objects are implemented
            ex._attrs[base_errors.FIELD_LINE_START] = str(
                err['locations'][0]['line']).encode()
            ex._attrs[base_errors.FIELD_COLUMN_START] = str(
                err['locations'][0]['column']).encode()

        raise ex

    def assert_graphql_query_result(self, query, result, *,
                                    msg=None, sort=None,
                                    operation_name=None,
                                    use_http_post=True,
                                    variables=None,
                                    globals=None,
                                    deprecated_globals=None):
        res = self.graphql_query(
            query,
            operation_name=operation_name,
            use_http_post=use_http_post,
            variables=variables,
            globals=globals,
            deprecated_globals=deprecated_globals)

        if sort is not None:
            # GQL will always have a single object returned. The data is
            # in the top-level fields, so that's what needs to be sorted.
            for r in res.values():
                assert_data_shape.sort_results(r, sort)

        assert_data_shape.assert_data_shape(
            res, result, self.fail, message=msg)
        return res
