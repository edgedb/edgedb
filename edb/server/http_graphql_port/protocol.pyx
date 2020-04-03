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


import json
import logging
import urllib.parse
from typing import Any, Dict, Tuple, List, Optional

from graphql.language import lexer as gql_lexer

from edb import _graphql_rewrite
from edb import errors
from edb.graphql import errors as gql_errors
from edb.server.pgcon import errors as pgerrors

from edb.common import debug
from edb.common import markup

from edb.server.http import http
from edb.server.http cimport http

from . import explore
from . import compiler


logger = logging.getLogger(__name__)
_USER_ERRORS = (
    _graphql_rewrite.LexingError,
    _graphql_rewrite.SyntaxError,
    _graphql_rewrite.NotFoundError,
)


cdef class Protocol(http.HttpProtocol):

    def __init__(self, loop, server, query_cache):
        http.HttpProtocol.__init__(self, loop)
        self.server = server
        self.query_cache = query_cache

    async def handle_request(self, http.HttpRequest request,
                             http.HttpResponse response):
        url_path = request.url.path.strip(b'/')

        if url_path == b'explore' and request.method == b'GET':
            response.body = explore.EXPLORE_HTML
            response.content_type = b'text/html'
            return

        if url_path != b'':
            response.body = f'Unknown path: /{url_path.decode()!r}'.encode()
            response.status = http.HTTPStatus.NOT_FOUND
            response.close_connection = True
            return

        operation_name = None
        variables = None
        query = None

        try:
            if request.method == b'POST':
                if request.content_type and b'json' in request.content_type:
                    body = json.loads(request.body)
                    if not isinstance(body, dict):
                        raise TypeError(
                            'the body of the request must be a JSON object')
                    query = body.get('query')
                    operation_name = body.get('operationName')
                    variables = body.get('variables')
                elif request.content_type == 'application/graphql':
                    query = request.body.decode('utf-8')
                else:
                    raise TypeError(
                        'unable to interpret GraphQL POST request')

            elif request.method == b'GET':
                if request.url.query:
                    url_query = request.url.query.decode('ascii')
                    qs = urllib.parse.parse_qs(url_query)

                    query = qs.get('query')
                    if query is not None:
                        query = query[0]

                    operation_name = qs.get('operationName')
                    if operation_name is not None:
                        operation_name = operation_name[0]

                    variables = qs.get('variables')
                    if variables is not None:
                        try:
                            variables = json.loads(variables[0])
                        except Exception:
                            raise TypeError(
                                '"variables" must be a JSON object')

            else:
                raise TypeError('expected a GET or a POST request')

            if not query:
                raise TypeError('invalid GraphQL request: query is missing')

            if (operation_name is not None and
                    not isinstance(operation_name, str)):
                raise TypeError('operationName must be a string')

            if variables is not None and not isinstance(variables, dict):
                raise TypeError('"variables" must be a JSON object')

        except Exception as ex:
            if debug.flags.server:
                markup.dump(ex)

            response.body = str(ex).encode()
            response.status = http.HTTPStatus.BAD_REQUEST
            response.close_connection = True
            return

        response.status = http.HTTPStatus.OK
        response.content_type = b'application/json'
        try:
            result = await self.execute(query, operation_name, variables)
        except Exception as ex:
            if debug.flags.server:
                markup.dump(ex)

            ex_type = type(ex)
            if issubclass(ex_type, (gql_errors.GraphQLError,
                                    pgerrors.BackendError)):
                # XXX Fix this when LSP "location" objects are implemented
                ex_type = errors.QueryError

            err_dct = {
                'message': f'{ex_type.__name__}: {ex}',
            }

            if (isinstance(ex, errors.EdgeDBError) and
                    hasattr(ex, 'line') and
                    hasattr(ex, 'col')):
                err_dct['locations'] = [{'line': ex.line, 'column': ex.col}]

            response.body = json.dumps({'errors': [err_dct]}).encode()
        else:
            response.body = b'{"data":' + result + b'}'

    async def compile(self,
            dbver: int,
            query: str,
            tokens: Optional[List[Tuple[int, int, int, str]]],
            substitutions: Optional[Dict[str, Tuple[str, int, int]]],
            operation_name: Optional[str],
            variables: Dict[str, Any],
        ):
        compiler = await self.server.compilers.get()
        try:
            return await compiler.call(
                'compile_graphql',
                dbver,
                query,
                tokens,
                substitutions,
                operation_name,
                variables)
        finally:
            self.server.compilers.put_nowait(compiler)

    async def execute(self, query, operation_name, variables):
        dbver = self.server.get_dbver()

        if debug.flags.graphql_compile:
            debug.header('Input graphql')
            print(query)
            print(f'variables: {variables}')

        try:
            rewritten = _graphql_rewrite.rewrite(operation_name, query)
        except Exception as e:
            if isinstance(e, _USER_ERRORS):
                logger.info("Error rewriting graphql query: %s", e)
            else:
                logger.warning("Error rewriting graphql query: %s", e)
            rewritten = None
            rewrite_error = e
            prepared_query = query
            vars = variables.copy() if variables else {}
        else:
            prepared_query = rewritten.key()
            vars = rewritten.variables().copy()
            if variables:
                vars.update(variables)

            if debug.flags.graphql_compile:
                debug.header('GraphQL optimized query')
                print(rewritten.key())
                print(f'variables: {vars}')

        cache_key = (prepared_query, operation_name, dbver)
        use_prep_stmt = False

        op: compiler.CompiledOperation = self.query_cache.get(
            cache_key, None)

        if op is None:
            if rewritten is not None:
                op = await self.compile(
                    dbver, query,
                    rewritten.tokens(gql_lexer.TokenKind),
                    rewritten.substitutions(),
                    operation_name, vars)
            else:
                op = await self.compile(
                    dbver, query, None, None, operation_name, vars)
            self.query_cache[cache_key] = op
        else:
            if op.cache_deps_vars:
                op = await self.compile(dbver,
                    query,
                    rewritten.tokens(gql_lexer.TokenKind),
                    rewritten.substitutions(),
                    operation_name, vars)
            else:
                # This is at least the second time this query is used
                # and it's safe to cache.
                use_prep_stmt = True

        args = []
        if op.sql_args:
            for name in op.sql_args:
                if name not in vars:
                    default = op.variables.get(name)
                    if default is None:
                        raise errors.QueryError(
                            f'no value for the {name!r} variable')
                    args.append(default)
                else:
                    args.append(vars[name])

        pgcon = await self.server.pgcons.get()
        try:
            data = await pgcon.parse_execute_json(
                op.sql, op.sql_hash, op.dbver,
                use_prep_stmt, args)
        finally:
            self.server.pgcons.put_nowait(pgcon)

        if data is None:
            raise errors.InternalServerError(
                f'no data received for a JSON query {op.sql!r}')

        return data
