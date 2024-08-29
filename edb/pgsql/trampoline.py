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


"""Support for namespacing and trampolining the standard library.

The idea here is that all of the functions, tables, and views in
edgedb/edgedbstd/edgedbsql should be moved into namespaced libraries
of the form `edgedb_VER`, where VER will be substituted with some
version identifier.

Then, for anything that might be referenced by a function, constraint,
etc, we will create a *trampoline* in the un-suffixed namespace.
When doing (eventually) in-place version upgrades, we will create the
new namespace and then update the trampolines to point to it.

CURRENT STATUS: So far, functions and views are mostly
namespaced. Standard library schema object tables aren't yet.
"""

from __future__ import annotations
from typing import (
    TYPE_CHECKING,
)

import abc
import copy
import dataclasses


from . import common
from . import dbops


q = common.qname
qi = common.quote_ident
ql = common.quote_literal


V = common.versioned_schema


def fixup_query(query: str) -> str:
    for s in common.VERSIONED_SCHEMAS:
        query = query.replace(f"{s}_VER", V(s))
    return query


class VersionedFunction(dbops.Function):
    if not TYPE_CHECKING:
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.name = (
                common.maybe_versioned_schema(self.name[0]), *self.name[1:])
            self.text = fixup_query(self.text)

            if self.args:
                nargs = []
                for arg in self.args:
                    if isinstance(arg, tuple) and isinstance(arg[1], tuple):
                        new_name = (
                            arg[1][0].replace('_VER', V('')), *arg[1][1:])
                        arg = (arg[0], new_name, *arg[2:])
                    nargs.append(arg)
                self.args = nargs


class VersionedView(dbops.View):
    if not TYPE_CHECKING:
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.name = (
                common.maybe_versioned_schema(self.name[0]), *self.name[1:])
            self.query = fixup_query(self.query)


@dataclasses.dataclass
class Trampoline:
    name: tuple[str, str]

    @abc.abstractmethod
    def make(self) -> dbops.Command:
        pass


@dataclasses.dataclass
class TrampolineFunction(Trampoline):
    func: dbops.Function

    def make(self) -> dbops.Command:
        return dbops.CreateFunction(self.func, or_replace=True)


@dataclasses.dataclass
class TrampolineView(Trampoline):
    old_name: tuple[str, str]

    def make(self) -> dbops.Command:
        return dbops.Query(f'''
            PERFORM {V('edgedb')}._create_trampoline_view(
                {ql(q(*self.old_name))}, {ql(self.name[0])}, {ql(self.name[1])})
        ''')


def make_trampoline(func: dbops.Function) -> TrampolineFunction:
    new_func = copy.copy(func)
    schema, name = func.name
    namespace = V('')
    assert schema.endswith(namespace), schema
    new_func.name = (schema[:-len(namespace)], name)

    args = []
    for arg in (func.args or ()):
        if isinstance(arg, str):
            args.append(arg)
        else:
            assert arg[0]
            args.append(arg[0])
    args = [qi(arg) for arg in args]
    if func.has_variadic:
        args[-1] = f'VARIADIC {args[-1]}'

    new_func.text = f'select {q(*func.name)}({", ".join(args)})'
    new_func.language = 'sql'
    new_func.strict = False
    return TrampolineFunction(new_func.name, new_func)


def make_table_trampoline(fullname: tuple[str, str]) -> TrampolineView:
    schema, name = fullname
    namespace = V('')
    assert schema.endswith(namespace), schema
    new_name = (schema[:-len(namespace)], name)

    return TrampolineView(new_name, fullname)


def make_view_trampoline(view: dbops.View) -> TrampolineView:
    return make_table_trampoline(view.name)
