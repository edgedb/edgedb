#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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
import enum
import functools

from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as ql_compiler
from edb.edgeql import parser as ql_parser

from edb.ir import staeval as ireval


class ConfigLevel(enum.Flag):

    SYSTEM = enum.auto()
    SESSION = enum.auto()


@dataclasses.dataclass(frozen=True)
class setting:

    type: type
    default: object
    level: ConfigLevel

    def is_system(self):
        return bool(self.level & ConfigLevel.SYSTEM)

    def value_from_qlast(self, std_schema, ql: qlast.Expr):
        try:
            val_ir = ql_compiler.compile_ast_fragment_to_ir(
                ql, schema=std_schema)
            val = ireval.evaluate_to_python_val(
                val_ir.expr, schema=std_schema)
        except ireval.StaticEvaluationError:
            raise errors.QueryError('invalid CONFIG value')
        else:
            if not isinstance(val, self.type):
                dispname = val_ir.stype.get_displayname(std_schema)
                raise errors.ConfigurationError(
                    f'expected a {self.type.__name__} value, '
                    f'got {dispname!r}')
            return val

    @functools.lru_cache()
    def value_from_eql(self, std_schema: str, eql: str):
        try:
            ql = ql_parser.parse_fragment(eql)
        except Exception:
            raise errors.QueryError('invalid CONFIG value')
        return self.value_from_qlast(std_schema, ql)
