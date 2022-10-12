#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2010-present MagicStack Inc. and the EdgeDB authors.
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


from .exceptions import PSqlParseError

cdef extern from "pg_query.h":
    ctypedef struct PgQueryError:
        char *message
        int lineno
        int cursorpos

    ctypedef struct PgQueryParseResult:
        char *parse_tree
        PgQueryError *error

    PgQueryParseResult pg_query_parse(const char* input)

    void pg_query_free_parse_result(PgQueryParseResult result);


def pg_parse(query) -> str:
    cdef PgQueryParseResult result

    result = pg_query_parse(query)
    if result.error:
        error = PSqlParseError(
            result.error.message.decode('utf8'),                   
            result.error.lineno, result.error.cursorpos
        )
        pg_query_free_parse_result(result)
        raise error

    result_utf8 = result.parse_tree.decode('utf8')
    pg_query_free_parse_result(result)
    return result_utf8
