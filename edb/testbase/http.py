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

import contextlib
import http.client
import json
import urllib.parse
import urllib.request

import edgedb

from edb.errors import base as base_errors
from edb.server import cluster

from . import server


class StubbornHttpConnection(http.client.HTTPConnection):

    def close(self):
        # Don't actually close the connection.  This allows us to
        # test keep-alive and "Connection: close" headers.
        pass

    def true_close(self):
        http.client.HTTPConnection.close(self)


class BaseHttpTest:

    # Serialize tests over http -- we don't want all tests workers to
    # create http ports. Every port spawns a number of Python
    # compiler processes and at a high number of parallel test
    # workers this ends up with test consuming too much system resources
    # for no particular speedup.
    SERIALIZED = True

    @classmethod
    def get_port_proto(cls):
        raise NotImplementedError

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.http_host = '127.0.0.1'
        cls.http_port = cluster.find_available_port()
        dbname = cls.get_database_name()

        cls.loop.run_until_complete(
            cls.con.execute(
                f'''
                    CONFIGURE SYSTEM INSERT Port {{
                        protocol := "{cls.get_port_proto()}",
                        database := "{dbname}",
                        address := "{cls.http_host}",
                        port := {cls.http_port},
                        user := "http",
                        concurrency := 4,
                    }};
                '''))

        cls.http_addr = f'http://127.0.0.1:{cls.http_port}'

    @contextlib.contextmanager
    def http_con(self):
        con = StubbornHttpConnection(self.http_host, self.http_port)
        con.connect()
        try:
            yield con
        finally:
            con.true_close()

    def http_con_send_request(self, con, params: dict, *, path=''):
        con.request(
            'GET',
            f'{self.http_addr}/{path}?{urllib.parse.urlencode(params)}')

    def http_con_read_response(self, con):
        resp = con.getresponse()
        resp_body = resp.read()
        resp_headers = {k.lower(): v.lower() for k, v in resp.getheaders()}
        return resp_body, resp_headers, resp.status

    def http_con_request(self, con, params: dict, *, path=''):
        self.http_con_send_request(con, params, path=path)
        return self.http_con_read_response(con)


class EdgeQLTestCase(BaseHttpTest, server.QueryTestCase):

    @classmethod
    def get_port_proto(cls):
        return 'edgeql+http'

    def edgeql_query(self, query, *, use_http_post=True, variables=None):
        req_data = {
            'query': query
        }

        if use_http_post:
            if variables is not None:
                req_data['variables'] = variables
            req = urllib.request.Request(self.http_addr, method='POST')
            req.add_header('Content-Type', 'application/json')
            response = urllib.request.urlopen(
                req, json.dumps(req_data).encode())
            resp_data = json.loads(response.read())
        else:
            if variables is not None:
                req_data['variables'] = json.dumps(variables)
            response = urllib.request.urlopen(
                f'{self.http_addr}/?{urllib.parse.urlencode(req_data)}')
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
                                   variables=None):
        res = self.edgeql_query(
            query,
            use_http_post=use_http_post,
            variables=variables)

        if sort is not None:
            # GQL will always have a single object returned. The data is
            # in the top-level fields, so that's what needs to be sorted.
            for r in res.values():
                self._sort_results(r, sort)

        self._assert_data_shape(res, result, message=msg)
        return res


class GraphQLTestCase(BaseHttpTest, server.QueryTestCase):

    @classmethod
    def get_port_proto(cls):
        return 'graphql+http'

    def graphql_query(self, query, *, operation_name=None,
                      use_http_post=True,
                      variables=None):
        req_data = {
            'query': query
        }

        if operation_name is not None:
            req_data['operationName'] = operation_name

        if use_http_post:
            if variables is not None:
                req_data['variables'] = variables
            req = urllib.request.Request(self.http_addr, method='POST')
            req.add_header('Content-Type', 'application/json')
            response = urllib.request.urlopen(
                req, json.dumps(req_data).encode())
            resp_data = json.loads(response.read())
        else:
            if variables is not None:
                req_data['variables'] = json.dumps(variables)
            response = urllib.request.urlopen(
                f'{self.http_addr}/?{urllib.parse.urlencode(req_data)}')
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
            ex._attrs[base_errors.FIELD_LINE] = str(
                err['locations'][0]['line']).encode()
            ex._attrs[base_errors.FIELD_COLUMN] = str(
                err['locations'][0]['column']).encode()

        raise ex

    def assert_graphql_query_result(self, query, result, *,
                                    msg=None, sort=None,
                                    operation_name=None,
                                    use_http_post=True,
                                    variables=None):
        res = self.graphql_query(
            query,
            operation_name=operation_name,
            use_http_post=use_http_post,
            variables=variables)

        if sort is not None:
            # GQL will always have a single object returned. The data is
            # in the top-level fields, so that's what needs to be sorted.
            for r in res.values():
                self._sort_results(r, sort)

        self._assert_data_shape(res, result, message=msg)
        return res
