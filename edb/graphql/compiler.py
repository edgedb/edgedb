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
from typing import Any, Optional, Tuple, Mapping, Dict, List

import functools

from edb import graphql

from edb.schema import schema as s_schema

from graphql.language import lexer as gql_lexer


@functools.lru_cache()
def _get_gqlcore(
    std_schema: s_schema.FlatSchema,
    user_schema: s_schema.FlatSchema,
    global_schema: s_schema.FlatSchema,
) -> graphql.GQLCoreSchema:
    return graphql.GQLCoreSchema(
        s_schema.ChainedSchema(
            std_schema,
            user_schema,
            global_schema
        )
    )


def compile_graphql(
    std_schema: s_schema.FlatSchema,
    user_schema: s_schema.FlatSchema,
    global_schema: s_schema.FlatSchema,
    database_config: Mapping[str, Any],
    system_config: Mapping[str, Any],
    gql: str,
    tokens: Optional[
        List[Tuple[gql_lexer.TokenKind, int, int, int, int, str]]],
    substitutions: Optional[Dict[str, Tuple[str, int, int]]],
    operation_name: Optional[str] = None,
    variables: Optional[Mapping[str, object]] = None,
) -> graphql.TranspiledOperation:
    if tokens is None:
        ast = graphql.parse_text(gql)
    else:
        ast = graphql.parse_tokens(gql, tokens)

    gqlcore = _get_gqlcore(std_schema, user_schema, global_schema)

    return graphql.translate_ast(
        gqlcore,
        ast,
        variables=variables,
        substitutions=substitutions,
        operation_name=operation_name,
    )
