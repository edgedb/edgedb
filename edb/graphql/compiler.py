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

import dataclasses
import functools
import hashlib

from edb import errors
from edb import graphql

from edb.schema import schema as s_schema

from graphql.language import lexer as gql_lexer

from edb.common import debug
from edb.edgeql import compiler as qlcompiler
from edb.pgsql import compiler as pg_compiler


@dataclasses.dataclass(frozen=True)
class CompiledOperation:

    sql: bytes
    sql_hash: bytes
    sql_args: List[str]
    cacheable: bool
    cache_deps_vars: Optional[FrozenSet[str]]
    variables: Dict


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
    operation_name: str=None,
    variables: Optional[Mapping[str, object]]=None,
) -> CompiledOperation:
    if tokens is None:
        ast = graphql.parse_text(gql)
    else:
        ast = graphql.parse_tokens(gql, tokens)

    gqlcore = _get_gqlcore(std_schema, user_schema, global_schema)

    op = graphql.translate_ast(
        gqlcore,
        ast,
        variables=variables,
        substitutions=substitutions,
        operation_name=operation_name)

    ir = qlcompiler.compile_ast_to_ir(
        op.edgeql_ast,
        schema=s_schema.ChainedSchema(
            std_schema,
            user_schema,
            global_schema,
        ),
        options=qlcompiler.CompilerOptions(
            json_parameters=True,
            allow_top_level_shape_dml=True,
        ),
    )

    if ir.cardinality.is_multi():
        raise errors.ResultCardinalityMismatchError(
            f'compiled GrqphQL query has cardinality {ir.cardinality}, '
            f'expected ONE')

    sql_text, argmap = pg_compiler.compile_ir_to_sql(
        ir,
        pretty=bool(debug.flags.edgeql_compile),
        expected_cardinality_one=True,
        output_format=pg_compiler.OutputFormat.JSON)

    args: List[Optional[str]] = [None] * len(argmap)
    for argname, param in argmap.items():
        args[param.index - 1] = argname

    sql_bytes = sql_text.encode()
    sql_hash = hashlib.sha1(sql_bytes).hexdigest().encode('latin1')

    return CompiledOperation(
        sql=sql_bytes,
        sql_hash=sql_hash,
        sql_args=args,  # type: ignore[arg-type]  # XXX: optional bug?
        cacheable=op.cacheable,
        cache_deps_vars=op.cache_deps_vars,
        variables=op.variables_desc,
    )
