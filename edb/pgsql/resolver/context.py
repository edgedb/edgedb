#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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

from edb.common import ast

from copy import deepcopy
from typing import *
import enum

from edb.common import compiler
from edb.schema import schema as s_schema
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers


class ContextSwitchMode(enum.Enum):
    EMPTY = enum.auto()
    ISOLATED = enum.auto()


class Scope:
    tables: List[Table]

    def __init__(self):
        self.tables = []


class Table:
    """Public SQL"""

    name: Optional[str] = None
    alias: Optional[str] = None

    columns: List[Column]

    """Internal SQL"""
    reference_as: Optional[str] = None

    def __init__(self):
        self.columns = []


class Column:
    """Public SQL"""

    name: Optional[str] = None

    """Internal SQL"""
    reference_as: Optional[str] = None


class NameGenerator:
    next_rel_index = 0

    def generate_relation(self) -> str:
        name = f'_rel_{self.next_rel_index}'
        self.next_rel_index += 1
        return name


class ResolverContextLevel(compiler.ContextLevel):
    schema: s_schema.Schema
    names: NameGenerator

    """Visible names in scope"""
    scope: Scope

    """Current table"""
    rel: Table
    include_inherited: bool

    def __init__(
        self,
        prevlevel: Optional[ResolverContextLevel],
        mode: ContextSwitchMode,
        *,
        schema: Optional[s_schema.Schema] = None,
    ) -> None:
        if prevlevel is None:
            assert schema is not None

            self.schema = schema
            self.scope = Scope()
            self.rel = Table()
            self.include_inherited = True
            self.names = NameGenerator()

        else:
            self.schema = prevlevel.schema
            self.names = prevlevel.names

            self.include_inherited = True

            if mode == ContextSwitchMode.EMPTY:
                self.scope = Scope()
                self.rel = Table()
            elif mode == ContextSwitchMode.ISOLATED:
                self.scope = deepcopy(prevlevel.scope)
                self.rel = deepcopy(prevlevel.rel)

    def empty(
        self,
    ) -> compiler.CompilerContextManager[ResolverContextLevel]:
        """Create a new empty context"""
        return self.new(ContextSwitchMode.EMPTY)

    def isolated(
        self,
    ) -> compiler.CompilerContextManager[ResolverContextLevel]:
        """Clone current context, prevent changes from leaking to parent"""
        return self.new(ContextSwitchMode.ISOLATED)


class ResolverContext(compiler.CompilerContext[ResolverContextLevel]):
    ContextLevelClass = ResolverContextLevel
    default_mode = ContextSwitchMode.EMPTY
