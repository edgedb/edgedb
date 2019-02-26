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


import dataclasses
import typing

from edb import graphql
from edb.common import debug
from edb.edgeql import compiler as ql_compiler
from edb.pgsql import compiler as pg_compiler
from edb.server import compiler


@dataclasses.dataclass(frozen=True)
class CompilerDatabaseState(compiler.CompilerDatabaseState):

    gqlcore: graphql.GQLCoreSchema


class Compiler(compiler.Compiler):

    def _wrap_schema(self, dbver, con_args, schema) -> CompilerDatabaseState:
        gqlcore = graphql.GQLCoreSchema(schema)
        return CompilerDatabaseState(
            dbver=dbver,
            con_args=con_args,
            schema=schema,
            gqlcore=gqlcore)

    async def compile_graphql(
            self,
            dbver: int,
            gql: str,
            operation_name: str=None,
            variables: typing.Optional[typing.Mapping[str, object]]=None):

        db = await self._get_database(dbver)

        trans = graphql.translate(
            db.gqlcore,
            gql,
            variables=variables)

        result = {}
        for op_name, op_desc in trans.items():
            ir = ql_compiler.compile_ast_to_ir(
                op_desc['edgeql'],
                schema=db.schema,
                json_parameters=True)

            sql_text, argmap = pg_compiler.compile_ir_to_sql(
                ir,
                pretty=debug.flags.edgeql_compile,
                output_format=pg_compiler.OutputFormat.NATIVE)

            args = [None] * len(argmap)
            for argname, argpos in argmap.items():
                args[argpos - 1] = argname

            result[op_name] = {
                'sql': sql_text.encode(),
                'args': args,
                'cacheable': op_desc['cacheable'],
                'cache_deps_vars': op_desc['cache_deps_vars'],
                'variables_desc': op_desc['variables_desc'],
            }

        return result
