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

from .compiler import Compiler
from .compiler import CompileContext, CompilerDatabaseState
from .compiler import compile_edgeql_script
from .compiler import load_std_schema
from .compiler import new_compiler, new_compiler_context
from .dbstate import QueryUnit, QueryUnitGroup
from .enums import Capability, Cardinality
from .enums import InputFormat, OutputFormat


__all__ = (
    'Cardinality',
    'Compiler',
    'CompileContext',
    'CompilerDatabaseState',
    'QueryUnit',
    'QueryUnitGroup',
    'Capability',
    'InputFormat',
    'OutputFormat',
    'compile_edgeql_script',
    'load_std_schema',
    'new_compiler',
    'new_compiler_context',
)
