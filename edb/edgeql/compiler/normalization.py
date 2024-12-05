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
from typing import (
    Any,
    Optional,
    Tuple,
    Union,
    AbstractSet,
    Mapping,
    Collection,
    Dict,
    List,
    cast,
)

import functools

from edb.common.ast import base

from edb.edgeql import ast as qlast
from edb.edgeql import parser as qlparser

from edb.schema import name as sn
from edb.schema import schema as s_schema
from edb.schema import utils as s_utils


@functools.singledispatch
def normalize(
    node: Any,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> None:
    raise AssertionError(f'normalize: cannot handle {node!r}')


def renormalize_compat(
    norm_qltree: qlast.Base_T,
    orig_text: str,
    *,
    schema: s_schema.Schema,
    localnames: AbstractSet[str] = frozenset(),
) -> qlast.Base_T:
    """Renormalize an expression normalized with imprint_expr_context().

    This helper takes the original, unmangled expression, an EdgeQL AST
    tree of the same expression mangled with `imprint_expr_context()`
    (which injects extra WITH MODULE clauses), and produces a normalized
    expression with explicitly qualified identifiers instead.  Old dumps
    are the main user of this facility.
    """
    orig_qltree = qlparser.parse_fragment(orig_text)

    norm_aliases: Dict[Optional[str], str] = {}
    assert isinstance(norm_qltree, (
        qlast.Query, qlast.Command, qlast.DDLCommand
    ))
    for alias in (norm_qltree.aliases or ()):
        if isinstance(alias, qlast.ModuleAliasDecl):
            norm_aliases[alias.alias] = alias.module

    if isinstance(orig_qltree, (
        qlast.Query, qlast.Command, qlast.DDLCommand
    )):
        orig_aliases: Dict[Optional[str], str] = {}
        for alias in (orig_qltree.aliases or ()):
            if isinstance(alias, qlast.ModuleAliasDecl):
                orig_aliases[alias.alias] = alias.module

        modaliases = {
            k: v
            for k, v in norm_aliases.items()
            if k not in orig_aliases
        }
    else:
        modaliases = norm_aliases

    normalize(
        orig_qltree,
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
    )

    assert isinstance(orig_qltree, type(norm_qltree))
    return cast(qlast.Base_T, orig_qltree)


def _normalize_recursively(
    node: qlast.Base,
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
    elif isinstance(value, (tuple, list)):
        if value and isinstance(value[0], qlast.Base):
            for el in value:
                normalize(
                    el,
                    schema=schema,
                    modaliases=modaliases,
                    localnames=localnames,
                )


@normalize.register
def normalize_generic(
    node: qlast.Base,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
    skip: Collection[str] = frozenset(),
) -> None:
    for field, value in base.iter_fields(node):
        if field not in skip:
            _normalize_recursively(
                node,
                value,
                schema=schema,
                modaliases=modaliases,
                localnames=localnames,
            )


# This is the heart of the whole thing.
@normalize.register
def normalize_ObjectRef(
    ref: qlast.ObjectRef,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> None:
    if ref.name not in localnames:
        obj = schema.get(
            s_utils.ast_ref_to_name(ref),
            default=None,
            module_aliases=modaliases,
        )
        if obj is not None:
            name = obj.get_name(schema)
            assert isinstance(name, sn.QualName)
            ref.module = name.module
        elif ref.module in modaliases:
            # Even if the name was not resolved in the
            # schema it may be the name of the object
            # being defined, as such the default module
            # should be used. Names that must be ignored
            # (like aliases and parameters) have already
            # been filtered by the localnames.
            ref.module = modaliases[ref.module]


def _normalize_with_block(
    node: qlast.Query,
    *,
    field: str='aliases',
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> Tuple[Mapping[Optional[str], str], AbstractSet[str]]:

    # Update the default aliases, modaliases, and localnames.
    modaliases = dict(modaliases)
    newaliases: List[Union[qlast.AliasedExpr, qlast.ModuleAliasDecl]] = []

    aliases: Optional[List[qlast.AliasedExpr]] = getattr(node, field)
    for alias in (aliases or ()):
        if isinstance(alias, qlast.ModuleAliasDecl):
            if alias.alias:
                modaliases[alias.alias] = alias.module
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

    setattr(node, field, newaliases)

    return modaliases, localnames


def _normalize_aliased_field(
    node: Union[qlast.SubjectQuery, qlast.ReturningQuery],
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

    normalize_generic(
        node,
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
        skip=('aliases', 'result'),
    )


@normalize.register(qlast.InsertQuery)
@normalize.register(qlast.UpdateQuery)
@normalize.register(qlast.DeleteQuery)
def normalize_DML(
    node: qlast.InsertQuery | qlast.UpdateQuery | qlast.DeleteQuery,
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

    normalize_generic(
        node,
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
        skip=('aliases',),
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

    # Process the rest
    normalize_generic(
        node,
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
        skip=('aliases', 'iterator'),
    )


@normalize.register
def normalize_GroupQuery(
    node: qlast.GroupQuery,
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
        'subject',
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
    )

    modaliases, localnames = _normalize_with_block(
        node,
        field='using',
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
    )

    normalize_generic(
        node,
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
        skip=('aliases', 'subject', 'using'),
    )


@normalize.register
def normalize_FunctionCall(
    node: qlast.FunctionCall,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    localnames: AbstractSet[str] = frozenset(),
) -> None:

    if node.func not in localnames:
        name = (
            sn.UnqualName(node.func) if isinstance(node.func, str)
            else sn.QualName(*node.func)
        )
        funcs = schema.get_functions(
            name, default=tuple(), module_aliases=modaliases)
        if funcs:
            # As long as we found some functions, they will be from
            # the same module (the first valid resolved module for the
            # function name will mask "std").
            sname = funcs[0].get_shortname(schema)
            node.func = (sname.module, sname.name)

        elif modaliases and isinstance(name, sn.QualName):
            # Even if no function was found, apply the modaliases.
            # It is possible that a function without the modalias exists but
            # we don't want to find that.
            #
            # Eg.
            # module A {
            #     function foo() -> int64 using (1);
            # }
            # module B {}
            # module default {
            #     alias query := (with A as module B select A::foo() );
            # }
            current_module = (
                modaliases[None]
                if modaliases and None in modaliases else
                None
            )
            _, module = s_schema.apply_module_aliases(
                name.module, modaliases, current_module,
            )
            if module is not None:
                node.func = (module, name.name)

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
        if (
            # maintype names 'array', 'tuple', 'range', and 'multirange'
            # specifically should also be ignored
            node.maintype.name not in {
                'array', 'tuple', 'range', 'multirange',
            }
        ):
            normalize(
                node.maintype,
                schema=schema,
                modaliases=modaliases,
                localnames=localnames,
            )

    normalize_generic(
        node,
        schema=schema,
        modaliases=modaliases,
        localnames=localnames,
        skip=('maintype',),
    )
