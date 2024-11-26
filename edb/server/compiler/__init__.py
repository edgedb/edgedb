#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
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
# See the License for tbhe specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations

from .compiler import Compiler, CompilerState
from .compiler import CompileContext, CompilerDatabaseState
from .compiler import compile_edgeql_script
from .compiler import new_compiler, new_compiler_from_pg, new_compiler_context
from .compiler import compile, compile_schema_storage_in_delta
from .compiler import maybe_force_database_error
from .dbstate import QueryUnit, QueryUnitGroup
from .enums import Capability, Cardinality
from .enums import InputFormat, OutputFormat, InputLanguage
from .explain import analyze_explain_output
from .ddl import repair_schema
from .rpc import CompilationRequest

__all__ = (
    'Cardinality',
    'CompilationRequest',
    'Compiler',
    'CompilerState',
    'CompileContext',
    'CompilerDatabaseState',
    'QueryUnit',
    'QueryUnitGroup',
    'Capability',
    'InputFormat',
    'InputLanguage',
    'OutputFormat',
    'analyze_explain_output',
    'compile_edgeql_script',
    'maybe_force_database_error',
    'new_compiler',
    'new_compiler_from_pg',
    'new_compiler_context',
    'compile',
    'compile_schema_storage_in_delta',
    'repair_schema',
)
