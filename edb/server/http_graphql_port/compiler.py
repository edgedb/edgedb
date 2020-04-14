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

import dataclasses
from typing import *

from edb import errors
from edb import graphql

from edb.common import debug
from edb.edgeql import compiler as qlcompiler
from edb.pgsql import compiler as pg_compiler
from edb.server import compiler


@dataclasses.dataclass(frozen=True)
class CompilerDatabaseState(compiler.CompilerDatabaseState):

    gqlcore: graphql.GQLCoreSchema


@dataclasses.dataclass(frozen=True)
class CompiledOperation:

    sql: bytes
    sql_hash: bytes
    sql_args: List[str]
    dbver: int
    cacheable: bool
    cache_deps_vars: Optional[FrozenSet[str]]
    variables: Dict


class Compiler(compiler.BaseCompiler):

    def _wrap_schema(self, dbver, schema) -> CompilerDatabaseState:
        gqlcore = graphql.GQLCoreSchema(schema)
        return CompilerDatabaseState(
            dbver=dbver,
            schema=schema,
            gqlcore=gqlcore)

    async def compile_graphql(
        self,
        dbver: int,
        gql: str,
        tokens: Optional[List[Tuple[int, int, int, str]]],
        substitutions: Optional[Dict[str, Tuple[str, int, int]]],
        operation_name: str=None,
        variables: Optional[Mapping[str, object]]=None,
    ) -> CompiledOperation:

        db = await self._get_database(dbver)

        if tokens is None:
            ast = graphql.parse_text(gql)
        else:
            ast = graphql.parse_tokens(gql, tokens)
        op = graphql.translate_ast(
            db.gqlcore,
            ast,
            variables=variables,
            substitutions=substitutions,
            operation_name=operation_name)

        ir = qlcompiler.compile_ast_to_ir(
            op.edgeql_ast,
            schema=db.schema,
            options=qlcompiler.CompilerOptions(
                json_parameters=True,
            ),
        )

        if ir.cardinality.is_multi():
            raise errors.ResultCardinalityMismatchError(
                f'compiled GrqphQL query has cardinality {ir.cardinality}, '
                f'expected ONE')

        sql_text, argmap = pg_compiler.compile_ir_to_sql(
            ir,
            pretty=debug.flags.edgeql_compile,
            expected_cardinality_one=True,
            output_format=pg_compiler.OutputFormat.JSON)

        args = [None] * len(argmap)
        for argname, param in argmap.items():
            args[param.index - 1] = argname

        sql_bytes = sql_text.encode()
        sql_hash = self._hash_sql(sql_bytes)

        return CompiledOperation(
            sql=sql_bytes,
            sql_hash=sql_hash,
            sql_args=args,
            dbver=dbver,
            cacheable=op.cacheable,
            cache_deps_vars=op.cache_deps_vars,
            variables=op.variables_desc,
        )
