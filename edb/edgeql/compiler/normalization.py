#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either nodeess or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""EdgeQL expression normalization functions."""


from __future__ import annotations
from typing import *

import functools

from edb.common.ast import base
from edb.edgeql import ast as qlast
from edb.schema import schema as s_schema
from edb.schema import name as sn


@functools.singledispatch
def normalize(
    node: Any,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> None:
    raise AssertionError(f'normalize: cannot handle {node!r}')


def _normalize_recursively(
    node: qlast.Base,
    field: str,
    value: Any,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> None:
    # We only want to handle fields that need to be traversed
    # recursively: Base AST and lists. Other fields are essentially
    # expected to be processed by the more specific handlers.
    if isinstance(value, qlast.Base):
        normalize(
            value,
            schema=schema,
            modaliases=modaliases,
            localnames=localnames,
        )
    elif isinstance(value, list):
        if value and isinstance(value[0], qlast.Base):
            for el in value:
                normalize(
                    el,
                    schema=schema,
                    modaliases=modaliases,
                    localnames=localnames,
                )


@normalize.register
def normalize_Base(
    node: qlast.Base,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> None:
    for field, value in base.iter_fields(node):
        _normalize_recursively(
            node,
            field,
            value,
            schema=schema,
            modaliases=modaliases,
            localnames=localnames,
        )


@normalize.register
def normalize_DDL(
    node: qlast.DDL,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> None:
    raise AssertionError(f'normalize: cannot handle {node!r}')


def _normalize_with_block(
    node: qlast.Statement,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> Tuple[Mapping[Optional[str], str], AbstractSet[str]]:

    # Update the default aliases, modaliases, and localnames.
    modaliases = dict(modaliases)
    newaliases: List[Union[qlast.AliasedExpr, qlast.ModuleAliasDecl]] = []

    for alias in node.aliases:
        if isinstance(alias, qlast.ModuleAliasDecl):
            if alias.alias:
                modaliases[alias.alias] = alias.module
                newaliases.append(alias)
            else:
                modaliases[None] = alias.module
        else:
            assert isinstance(alias, qlast.AliasedExpr)
            normalize(
                alias.expr,
                schema=schema,
                modaliases=modaliases,
                localnames=localnames,
            )
            newaliases.append(alias)
            localnames = {alias.alias} | localnames

    node.aliases = newaliases

    return modaliases, localnames


def _normalize_aliased_field(
    node: Union[qlast.SubjectMixin, qlast.ReturningMixin, qlast.ForQuery],
    fname: str,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> AbstractSet[str]:

    # Potentially the result defines an alias that is visible in other
    # clauses
    val = getattr(node, fname)
    normalize(
        val,
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
    )
    alias = getattr(node, f'{fname}_alias', None)
    if alias:
        localnames = {alias} | localnames

    return localnames


@normalize.register
def normalize_SelectQuery(
    node: qlast.SelectQuery,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> None:

    # Process WITH block
    modaliases, localnames = _normalize_with_block(
        node,
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
    )

    # Process the result expression
    localnames = _normalize_aliased_field(
        node,
        'result',
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
    )

    for field in ('where', 'orderby', 'offset', 'limit'):
        value = getattr(node, field, None)
        _normalize_recursively(
            node,
            field,
            value,
            schema=schema,
            modaliases=modaliases,
            localnames=localnames,
        )


@normalize.register
def normalize_InsertQuery(
    node: qlast.InsertQuery,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> None:

    # Process WITH block
    modaliases, localnames = _normalize_with_block(
        node,
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
    )

    # Process the subject expression
    localnames = _normalize_aliased_field(
        node,
        'subject',
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
    )

    for field in ('shape',):
        value = getattr(node, field, None)
        _normalize_recursively(
            node,
            field,
            value,
            schema=schema,
            modaliases=modaliases,
            localnames=localnames,
        )


@normalize.register
def normalize_UpdateQuery(
    node: qlast.UpdateQuery,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> None:

    # Process WITH block
    modaliases, localnames = _normalize_with_block(
        node,
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
    )

    # Process the subject expression
    localnames = _normalize_aliased_field(
        node,
        'subject',
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
    )

    for field in ('where', 'shape',):
        value = getattr(node, field, None)
        _normalize_recursively(
            node,
            field,
            value,
            schema=schema,
            modaliases=modaliases,
            localnames=localnames,
        )


@normalize.register
def normalize_DeleteQuery(
    node: qlast.DeleteQuery,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> None:

    # Process WITH block
    modaliases, localnames = _normalize_with_block(
        node,
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
    )

    # Process the subject expression
    localnames = _normalize_aliased_field(
        node,
        'subject',
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
    )

    for field in ('where', 'orderby', 'offset', 'limit'):
        value = getattr(node, field, None)
        _normalize_recursively(
            node,
            field,
            value,
            schema=schema,
            modaliases=modaliases,
            localnames=localnames,
        )


@normalize.register
def normalize_ForQuery(
    node: qlast.ForQuery,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> None:

    # Process WITH block
    modaliases, localnames = _normalize_with_block(
        node,
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
    )

    # Process the iterator expression
    localnames = _normalize_aliased_field(
        node,
        'iterator',
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
    )

    # Process the result expression
    localnames = _normalize_aliased_field(
        node,
        'result',
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
    )


@normalize.register
def normalize_GroupQuery(
    node: qlast.GroupQuery,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> None:
    raise AssertionError(f'normalize: cannot handle {node!r}')


@normalize.register
def compile_Path(
    node: qlast.Path,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> None:

    for step in node.steps:
        if isinstance(step, (qlast.Expr, qlast.TypeIntersection)):
            normalize(
                step,
                schema=schema,
                modaliases=modaliases,
                localnames=localnames,
            )
        elif isinstance(step, qlast.ObjectRef):
            # This is a specific path root, resolve it.
            if not step.module and step.name not in localnames:
                obj = schema.get(
                    step.name,
                    default=None,
                    module_aliases=modaliases,
                )
                if obj is not None:
                    name = obj.get_name(schema)
                    assert isinstance(name, sn.Name)
                    step.module = name.module
                elif None in modaliases:
                    # Even if the name was not resolved in the
                    # schema it may be the name of the object
                    # being defined, as such the default module
                    # should be used. Names that must be ignored
                    # (like aliases and parameters) have already
                    # been filtered by the localnames.
                    step.module = modaliases[None]


@normalize.register
def compile_FunctionCall(
    node: qlast.FunctionCall,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> None:

    if isinstance(node.func, str) and node.func not in localnames:
        funcs = schema.get_functions(
            node.func, default=tuple(), module_aliases=modaliases)
        if funcs:
            # As long as we found some functions, they will be from
            # the same module (the first valid resolved module for the
            # function name will mask "std").
            _, fname = funcs[0].get_name(schema).as_tuple()
            _, module, name = sn.split_name(sn.shortname_from_fullname(fname))
            node.func = (module, name)

        # It's odd we don't find a function, but this will be picked up
        # by the compiler with a more appropriate error message.

    for arg in node.args:
        normalize(
            arg,
            schema=schema,
            modaliases=modaliases,
            localnames=localnames,
        )

    for val in node.kwargs.values():
        normalize(
            val,
            schema=schema,
            modaliases=modaliases,
            localnames=localnames,
        )


@normalize.register
def compile_TypeName(
    node: qlast.TypeName,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> None:

    # Resolve the main type
    if isinstance(node.maintype, qlast.ObjectRef):
        # This is a specific path root, resolve it.
        if (not node.maintype.module and
                # maintype names 'array' and 'tuple' specifically
                # should also be ignored
                node.maintype.name not in {'array', 'tuple', *localnames}):
            maintype = schema.get(
                node.maintype.name,
                default=None,
                module_aliases=modaliases,
            )

            if maintype is not None:
                name = maintype.get_name(schema)
                assert isinstance(name, sn.Name)
                node.maintype.module = name.module
            elif None in modaliases:
                # Even if the name was not resolved in the schema it
                # may be the name of the object being defined, as such
                # the default module should be used. Names that must
                # be ignored (like aliases and parameters) have
                # already been filtered by the localnames.
                node.maintype.module = modaliases[None]

    if node.subtypes is not None:
        for st in node.subtypes:
            normalize(
                st,
                schema=schema,
                modaliases=modaliases,
                localnames=localnames,
            )
