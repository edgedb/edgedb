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
from typing import *

import json
import urllib.parse
import urllib.request

import edgedb

from edb.errors import base as base_errors

from edb.common import assert_data_shape

from . import server


bag = assert_data_shape.bag


class BaseHttpExtensionTest(server.QueryTestCase):
    EXTENSION_SETUP: List[str] = []

    @classmethod
    def get_extension_name(cls):
        raise NotImplementedError

    @classmethod
    def get_extension_path(cls):
        return cls.get_extension_name()

    @classmethod
    def get_api_prefix(cls):
        extpath = cls.get_extension_path()
        dbname = cls.get_database_name()
        return f'/db/{dbname}/{extpath}'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        extname = cls.get_extension_name()
        cls.loop.run_until_complete(
            cls.con.execute(f'CREATE EXTENSION {extname};')
        )
        for q in cls.EXTENSION_SETUP:
            cls.loop.run_until_complete(cls.con.execute(q))

    @classmethod
    def tearDownClass(cls):
        extname = cls.get_extension_name()
        cls.loop.run_until_complete(
            cls.con.execute(f'DROP EXTENSION {extname};')
        )
        super().tearDownClass()


class ExtAuthTestCase(BaseHttpExtensionTest):

    @classmethod
    def get_extension_name(cls):
        return 'auth'

    @classmethod
    def get_extension_path(cls):
        return 'ext/auth'


class EdgeQLTestCase(BaseHttpExtensionTest):

    @classmethod
    def get_extension_name(cls):
        return 'edgeql_http'

    @classmethod
    def get_extension_path(cls):
        return 'edgeql'

    def edgeql_query(
            self, query, *, use_http_post=True, variables=None, globals=None):
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
            response = urllib.request.urlopen(
                req, json.dumps(req_data).encode(), context=self.tls_context
            )
            resp_data = json.loads(response.read())
        else:
            if variables is not None:
                req_data['variables'] = json.dumps(variables)
            if globals is not None:
                req_data['globals'] = json.dumps(globals)
            response = urllib.request.urlopen(
                f'{self.http_addr}/?{urllib.parse.urlencode(req_data)}',
                context=self.tls_context,
            )
            resp_data = json.loads(response.read())

        if 'data' in resp_data:
            return resp_data['data']

        err = resp_data['error']

        ex_msg = err['message'].strip()
        ex_code = err['code']

        raise edgedb.EdgeDBError._from_code(ex_code, ex_msg)

    def assert_edgeql_query_result(self, query, result, *,
                                   msg=None, sort=None,
                                   use_http_post=True,
                                   variables=None,
                                   globals=None):
        res = self.edgeql_query(
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

    @classmethod
    def get_extension_name(cls):
        return 'graphql'

    def graphql_query(self, query, *, operation_name=None,
                      use_http_post=True,
                      variables=None,
                      globals=None):
        req_data = {
            'query': query
        }

        if operation_name is not None:
            req_data['operationName'] = operation_name

        if use_http_post:
            if variables is not None:
                req_data['variables'] = variables
            if globals is not None:
                req_data['globals'] = globals
            req = urllib.request.Request(self.http_addr, method='POST')
            req.add_header('Content-Type', 'application/json')
            response = urllib.request.urlopen(
                req, json.dumps(req_data).encode(), context=self.tls_context
            )
            resp_data = json.loads(response.read())
        else:
            if variables is not None:
                req_data['variables'] = json.dumps(variables)
            if globals is not None:
                req_data['globals'] = json.dumps(globals)
            response = urllib.request.urlopen(
                f'{self.http_addr}/?{urllib.parse.urlencode(req_data)}',
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
                                    globals=None):
        res = self.graphql_query(
            query,
            operation_name=operation_name,
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
