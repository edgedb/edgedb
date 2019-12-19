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
from typing import *  # NoQA

from edb import errors
from edb import graphql

from edb.common import debug
from edb.edgeql import compiler as ql_compiler
from edb.edgeql import qltypes
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
    cache_deps_vars: Dict
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
            operation_name: str=None,
            variables: Optional[Mapping[str, object]]=None):

        db = await self._get_database(dbver)

        op = graphql.translate(
            db.gqlcore,
            gql,
            variables=variables,
            operation_name=operation_name)

        ir = ql_compiler.compile_ast_to_ir(
            op.edgeql_ast,
            schema=db.schema,
            json_parameters=True)

        if ir.cardinality is not qltypes.Cardinality.ONE:
            raise errors.ResultCardinalityMismatchError(
                f'compiled GrqphQL query has cardinality {ir.cardinality}, '
                f'expected ONE')

        sql_text, argmap = pg_compiler.compile_ir_to_sql(
            ir,
            pretty=debug.flags.edgeql_compile,
            expected_cardinality_one=True,
            output_format=pg_compiler.OutputFormat.JSON)

        args = [None] * len(argmap)
        for argname, argpos in argmap.items():
            args[argpos - 1] = argname

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
