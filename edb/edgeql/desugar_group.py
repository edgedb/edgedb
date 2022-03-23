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

"""Desugar GROUP queries into internal IGROUP queries.

This code is called by both the model and the real implementation,
though if that starts becoming a problem it should just be abandoned.
"""

from __future__ import annotations


from typing import *

from edb.common import ordered
from edb.common.compiler import AliasGenerator

from edb.edgeql import ast as qlast


def name_path(name: str) -> qlast.Path:
    return qlast.Path(steps=[qlast.ObjectRef(name=name)])


def make_free_object(els: Dict[str, qlast.Expr]) -> qlast.Shape:
    return qlast.Shape(
        expr=None,
        elements=[
            qlast.ShapeElement(
                expr=qlast.Path(
                    steps=[qlast.Ptr(ptr=qlast.ObjectRef(name=name))]),
                compexpr=expr
            )
            for name, expr in els.items()
        ],
    )


def collect_grouping_atoms(
        els: List[qlast.GroupingElement]) -> AbstractSet[str]:
    atoms: ordered.OrderedSet[str] = ordered.OrderedSet()

    def _collect_atom(el: qlast.GroupingAtom) -> None:
        if isinstance(el, qlast.GroupingIdentList):
            for at in el.elements:
                _collect_atom(at)

        else:
            assert isinstance(el, qlast.ObjectRef)
            atoms.add(el.name)

    def _collect_el(el: qlast.GroupingElement) -> None:
        if isinstance(el, qlast.GroupingSets):
            for sub in el.sets:
                _collect_el(sub)
        elif isinstance(el, qlast.GroupingOperation):
            for at in el.elements:
                _collect_atom(at)
        elif isinstance(el, qlast.GroupingSimple):
            _collect_atom(el.element)
        else:
            raise AssertionError('Unknown GroupingElement')

    for el in els:
        _collect_el(el)

    return atoms


def desugar_group(
    node: qlast.GroupQuery,
    aliases: AliasGenerator,
) -> qlast.InternalGroupQuery:
    assert not isinstance(node, qlast.InternalGroupQuery)
    alias_map: Dict[str, Tuple[str, qlast.Expr]] = {}

    def rewrite_atom(el: qlast.GroupingAtom) -> qlast.GroupingAtom:
        if isinstance(el, qlast.ObjectRef):
            return el
        elif isinstance(el, qlast.Path):
            assert isinstance(el.steps[0], qlast.Ptr)
            ptrname = el.steps[0].ptr.name
            if ptrname not in alias_map:
                alias = aliases.get(ptrname)
                alias_map[ptrname] = (alias, el)
            alias = alias_map[ptrname][0]
            return qlast.ObjectRef(name=alias)
        else:
            return qlast.GroupingIdentList(
                context=el.context,
                elements=tuple(rewrite_atom(at) for at in el.elements),
            )

    def rewrite(el: qlast.GroupingElement) -> qlast.GroupingElement:
        if isinstance(el, qlast.GroupingSimple):
            return qlast.GroupingSimple(
                context=el.context, element=rewrite_atom(el.element))
        elif isinstance(el, qlast.GroupingSets):
            return qlast.GroupingSets(
                context=el.context, sets=[rewrite(s) for s in el.sets])
        elif isinstance(el, qlast.GroupingOperation):
            return qlast.GroupingOperation(
                context=el.context,
                oper=el.oper,
                elements=[rewrite_atom(a) for a in el.elements])
        raise AssertionError

    for using_clause in (node.using or ()):
        alias_map[using_clause.alias] = (using_clause.alias, using_clause.expr)

    using = node.using[:] if node.using else []
    by = [rewrite(by_el) for by_el in node.by]
    for alias, path in alias_map.values():
        using.append(qlast.AliasedExpr(alias=alias, expr=path))

    actual_keys = collect_grouping_atoms(by)

    g_alias = aliases.get('g')
    grouping_alias = aliases.get('grouping')
    output_dict = {
        'key': make_free_object({
            name: name_path(alias)
            for name, (alias, _) in alias_map.items()
            if alias in actual_keys
        }),
        'grouping': qlast.FunctionCall(
            func='array_unpack',
            args=[name_path(grouping_alias)],
        ),
        'elements': name_path(g_alias),
    }
    output_shape = make_free_object(output_dict)

    return qlast.InternalGroupQuery(
        context=node.context,
        aliases=node.aliases,
        subject_alias=node.subject_alias,
        subject=node.subject,
        # rewritten parts!
        using=using,
        by=by,
        group_alias=g_alias,
        grouping_alias=grouping_alias,
        result=output_shape,
        from_desugaring=True,
    )


def try_group_rewrite(
    node: qlast.Query,
    aliases: AliasGenerator,
) -> Optional[qlast.Query]:
    """
    Try to apply some syntactic rewrites of GROUP expressions so we
    can generate better code.

    The two key desugarings are:

    * Sink a shape into the internal group result

        SELECT (GROUP ...) <shape>
        [filter-clause] [order-clause] [other clauses]
        =>
        SELECT (
          FOR GROUP ...
          UNION <igroup-body> <shape>
          [filter-clause]
          [order-clause]
        ) [other clauses]

    * Convert a FOR over a group into just an internal group (and
      a trivial FOR)

        FOR g in (GROUP ...) UNION <body>
        =>
        FOR GROUP ...
        UNION (
            FOR g IN (<group-body>)
            UNION <body>
        )
    """

    # TODO: would Python's new pattern matching fit well here???

    # Sink shapes into the GROUP
    if (
        isinstance(node, qlast.SelectQuery)
        and isinstance(node.result, qlast.Shape)
        and isinstance(node.result.expr, qlast.GroupQuery)
    ):
        igroup = desugar_group(node.result.expr, aliases)
        igroup = igroup.replace(result=qlast.Shape(
            expr=igroup.result, elements=node.result.elements))

        # FILTER gets sunk into the body of the FOR GROUP
        if node.where or node.orderby:
            igroup = igroup.replace(
                # We need to move the result_alias in case
                # the FILTER depends on it.
                result_alias=node.result_alias,
                where=node.where,
                orderby=node.orderby,
            )

        return node.replace(
            result=igroup, result_alias=None, where=None, orderby=None)

    # Eliminate FORs over GROUPs
    if (
        isinstance(node, qlast.ForQuery)
        and isinstance(node.iterator, qlast.GroupQuery)
    ):
        igroup = desugar_group(node.iterator, aliases)
        new_result = qlast.ForQuery(
            iterator_alias=node.iterator_alias,
            iterator=igroup.result,
            result=node.result,
        )
        return igroup.replace(result=new_result, aliases=node.aliases)

    return None
