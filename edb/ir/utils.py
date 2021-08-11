#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2015-present MagicStack Inc. and the EdgeDB authors.
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

"""Miscellaneous utilities for the IR."""


from __future__ import annotations
from typing import *

import json

from edb import errors

from edb.common import ast

from edb.edgeql import qltypes as ft

from . import ast as irast
from . import typeutils


def get_longest_paths(ir: irast.Base) -> Set[irast.Set]:
    """Return a distinct set of longest paths found in an expression.

    For example in SELECT (A.B.C, D.E.F, A.B, D.E) the result would
    be {A.B.C, D.E.F}.
    """
    result = set()
    parents = set()

    flt = lambda n: isinstance(n, irast.Set) and n.expr is None
    ir_sets = ast.find_children(ir, flt)
    for ir_set in ir_sets:
        result.add(ir_set)
        if ir_set.rptr:
            parents.add(ir_set.rptr.source)

    return result - parents


def get_parameters(ir: irast.Base) -> Set[irast.Parameter]:
    """Return all parameters found in *ir*."""
    result: Set[irast.Parameter] = set()
    flt = lambda n: isinstance(n, irast.Parameter)
    result.update(ast.find_children(ir, flt))
    return result


def is_const(ir: irast.Base) -> bool:
    """Return True if the given *ir* expression is constant."""
    flt = lambda n: isinstance(n, irast.Set) and n.expr is None and n is not ir
    ir_sets = ast.find_children(ir, flt)
    variables = get_parameters(ir)
    return not ir_sets and not variables


def is_coalesce_expr(ir: irast.Base) -> bool:
    """Return True if the given *ir* expression is a coalesce expression."""
    return (
        isinstance(ir, irast.OperatorCall) and
        ir.operator_kind is ft.OperatorKind.Infix and
        str(ir.func_shortname) == 'std::??'
    )


def is_set_membership_expr(ir: irast.Base) -> bool:
    """Return True if the given *ir* expression is a set membership test."""
    return (
        isinstance(ir, irast.OperatorCall) and
        ir.operator_kind is ft.OperatorKind.Infix and
        str(ir.func_shortname) in {'std::IN', 'std::NOT IN'}
    )


def is_distinct_expr(ir: irast.Base) -> bool:
    """Return True if the given *ir* expression is a DISTINCT expression."""
    return (
        isinstance(ir, irast.OperatorCall) and
        ir.operator_kind is ft.OperatorKind.Prefix and
        str(ir.func_shortname) == 'std::DISTINCT'
    )


def is_union_expr(ir: irast.Base) -> bool:
    """Return True if the given *ir* expression is a UNION expression."""
    return (
        isinstance(ir, irast.OperatorCall) and
        ir.operator_kind is ft.OperatorKind.Infix and
        str(ir.func_shortname) == 'std::UNION'
    )


def is_exists_expr(ir: irast.Base) -> bool:
    """Return True if the given *ir* expression is an EXISTS expression."""
    return (
        isinstance(ir, irast.OperatorCall) and
        ir.operator_kind is ft.OperatorKind.Prefix and
        str(ir.func_shortname) == 'std::EXISTS'
    )


def is_ifelse_expr(ir: irast.Base) -> bool:
    """Return True if the given *ir* expression is an IF expression."""
    return (
        isinstance(ir, irast.OperatorCall) and
        ir.operator_kind is ft.OperatorKind.Ternary and
        str(ir.func_shortname) == 'std::IF'
    )


def is_empty_array_expr(ir: irast.Base) -> bool:
    """Return True if the given *ir* expression is an empty array expression.
    """
    return (
        isinstance(ir, irast.Array)
        and not ir.elements
    )


def is_untyped_empty_array_expr(ir: irast.Base) -> bool:
    """Return True if the given *ir* expression is an empty
       array expression of an uknown type.
    """
    return (
        is_empty_array_expr(ir)
        and (ir.typeref is None                    # type: ignore
             or typeutils.is_generic(ir.typeref))  # type: ignore
    )


def is_empty(ir: irast.Base) -> bool:
    """Return True if the given *ir* expression is an empty set
       or an empty array.
    """
    return (
        isinstance(ir, irast.EmptySet) or
        (isinstance(ir, irast.Array) and not ir.elements) or
        (
            isinstance(ir, irast.Set)
            and ir.expr is not None
            and is_empty(ir.expr)
        )
    )


def is_subquery_set(ir_expr: irast.Base) -> bool:
    """Return True if the given *ir_expr* expression is a subquery."""
    return (
        isinstance(ir_expr, irast.Set) and
        isinstance(ir_expr.expr, irast.Stmt)
    )


def is_scalar_view_set(ir_expr: irast.Base) -> bool:
    """Return True if the given *ir_expr* expression is a view
       of scalar type.
    """
    return (
        isinstance(ir_expr, irast.Set) and
        len(ir_expr.path_id) == 1 and
        ir_expr.path_id.is_scalar_path() and
        ir_expr.path_id.is_view_path()
    )


def is_implicit_wrapper(ir_expr: irast.Base) -> bool:
    """Return True if the given *ir_expr* expression is an implicit
       SELECT wrapper.
    """
    return (
        isinstance(ir_expr, irast.SelectStmt) and
        ir_expr.implicit_wrapper
    )


def is_trivial_select(ir_expr: irast.Base) -> bool:
    """Return True if the given *ir_expr* expression is a trivial
       SELECT expression, i.e `SELECT <expr>`.
    """
    if not isinstance(ir_expr, irast.SelectStmt):
        return False

    return (
        not ir_expr.orderby
        and ir_expr.iterator_stmt is None
        and ir_expr.where is None
        and ir_expr.limit is None
        and ir_expr.offset is None
    )


def unwrap_set(ir_set: irast.Set) -> irast.Set:
    """If the give *ir_set* is an implicit SELECT wrapper, return the
       wrapped set.
    """
    if ir_set.expr is not None and is_implicit_wrapper(ir_set.expr):
        return ir_set.expr.result  # type: ignore
    else:
        return ir_set


def get_source_context_as_json(
    expr: irast.Base,
    exctype: Type[errors.EdgeDBError] = errors.InternalServerError,
) -> str:
    if expr.context:
        details = json.dumps({
            # TODO(tailhook) should we add offset, utf16column here?
            'line': expr.context.start_point.line,
            'column': expr.context.start_point.column,
            'name': expr.context.name,
            'code': exctype.get_code(),
        })

    else:
        details = json.dumps({
            'code': exctype.get_code(),
        })

    return details


def is_type_intersection_reference(ir_expr: irast.Base) -> bool:
    """Return True if the given *ir_expr* is a type intersection, i.e
       ``Foo[IS Type]``.
    """
    if not isinstance(ir_expr, irast.Set):
        return False

    rptr = ir_expr.rptr
    if rptr is None:
        return False

    ir_source = rptr.source

    if ir_source.path_id.is_type_intersection_path():
        source_is_type_intersection = True
    else:
        source_is_type_intersection = False

    return source_is_type_intersection


def collapse_type_intersection(
    ir_set: irast.Set,
) -> Tuple[irast.Set, List[irast.TypeIntersectionPointer]]:

    result: List[irast.TypeIntersectionPointer] = []

    source = ir_set
    while True:
        rptr = source.rptr
        if not isinstance(rptr, irast.TypeIntersectionPointer):
            break
        result.append(rptr)
        source = rptr.source

    return source, result


def get_nearest_dml_stmt(ir_set: irast.Set) -> Optional[irast.MutatingStmt]:
    """For a given *ir_set* representing a Path, return the nearest path
       step that is a DML expression.
    """
    cur_set: Optional[irast.Set] = ir_set
    while cur_set is not None:
        if isinstance(cur_set.expr, irast.MutatingStmt):
            return cur_set.expr
        elif isinstance(cur_set.expr, irast.SelectStmt):
            cur_set = cur_set.expr.result
        elif cur_set.rptr is not None:
            cur_set = cur_set.rptr.source
        else:
            cur_set = None
    return None


class ContainsDMLVisitor(ast.NodeVisitor):
    skip_hidden = True

    def __init__(self, *, skip_bindings: bool) -> None:
        super().__init__()
        self.skip_bindings = skip_bindings

    def combine_field_results(self, xs: List[Optional[bool]]) -> bool:
        return any(
            x is True
            or (isinstance(x, list) and self.combine_field_results(x))
            for x in xs
        )

    def visit_MutatingStmt(self, stmt: irast.MutatingStmt) -> bool:
        return True

    def visit_Set(self, node: irast.Set) -> bool:
        if self.skip_bindings and node.is_binding:
            return False

        # Visit sub-trees
        return bool(self.generic_visit(node))


def contains_dml(stmt: irast.Base, *, skip_bindings: bool=False) -> bool:
    """Check whether a statement contains any DML in a subtree."""
    # TODO: Make this caching.
    visitor = ContainsDMLVisitor(skip_bindings=skip_bindings)
    res = visitor.visit(stmt) is True
    return res


class FindPathScopes(ast.NodeVisitor):
    """Visitor to find the enclosing path scope id of sub expressions.

    Sets inherit an effective scope id from enclosing expressions,
    and this visitor computes those.

    This is set up so that another visitor could inherit from it,
    override process_set, and also collect the scope tree info.
    """
    def __init__(self) -> None:
        super().__init__()
        self.path_scope_ids: List[Optional[int]] = [None]
        self.scopes: Dict[irast.Set, Optional[int]] = {}

    def visit_Stmt(self, stmt: irast.Stmt) -> Any:
        # Sometimes there is sharing, so we want the official scope
        # for a node to be based on its appearance in the result,
        # not in a subquery.
        # I think it might not actually matter, though.
        self.visit(stmt.bindings)
        if stmt.iterator_stmt:
            self.visit(stmt.iterator_stmt)
        if isinstance(stmt, irast.MutatingStmt):
            self.visit(stmt.subject)
        self.visit(stmt.result)

        return self.generic_visit(stmt)

    def visit_Set(self, node: irast.Set) -> Any:
        val = self.path_scope_ids[-1]
        if node.path_scope_id:
            self.path_scope_ids.append(node.path_scope_id)
        if not node.is_binding:
            val = self.path_scope_ids[-1]

        # Visit sub-trees
        self.scopes[node] = val
        res = self.process_set(node)

        if node.path_scope_id:
            self.path_scope_ids.pop()

        return res

    def process_set(self, node: irast.Set) -> Any:
        self.generic_visit(node)
        return None


def find_path_scopes(stmt: irast.Base) -> Dict[irast.Set, Optional[int]]:
    visitor = FindPathScopes()
    visitor.visit(stmt)
    return visitor.scopes


class FindPotentiallyVisibleVisitor(FindPathScopes):
    skip_hidden = True
    extra_skips = frozenset(['materialized_sets'])

    def __init__(self, to_skip: AbstractSet[irast.PathId]) -> None:
        super().__init__()
        self.to_skip = to_skip

    def combine_field_results(self, xs: Any) -> Set[irast.Set]:
        out = set()
        for x in xs:
            if isinstance(x, list):
                x = self.combine_field_results(x)
            if x:
                if isinstance(x, set):
                    out.update(x)
        return out

    def process_set(self, node: irast.Set) -> Set[irast.Set]:
        if node.path_id in self.to_skip:
            return set()

        results = []
        results.append(self.visit(node.rptr))
        # If the root of a path is potentially visible,
        # say that the whole path is
        if self.combine_field_results(results):
            results.append({node})
        results.append(self.visit(node.shape))
        if not node.rptr:
            results.append(self.visit(node.expr))

        # Bound variables are always potentially visible as are object
        # references (which have no expr or rptr).
        if (
            node.is_binding
            or (not node.expr and not node.rptr)
        ):
            results.append({node})

        # Visit sub-trees
        return self.combine_field_results(results)


def find_potentially_visible(
    stmt: irast.Base,
    scope: irast.ScopeTreeNode,
    scope_tree_nodes: Mapping[int, irast.ScopeTreeNode],
    to_skip: AbstractSet[irast.PathId]=frozenset()
) -> Set[irast.PathId]:
    """Find all "potentially visible" sets referenced."""
    # TODO: Make this caching.
    visitor = FindPotentiallyVisibleVisitor(to_skip=to_skip)
    visible_sets = cast(Set[irast.Set], visitor.visit(stmt))

    visible_paths = set()
    for ir in visible_sets:
        path_id = ir.path_id
        # Collect any namespaces between where the set is referred to
        # and the binding point we are looking from, and strip those off.
        # We need to do this because visibility *from the binding point*
        # needs to not include namespaces defined below it.
        # (See test_edgeql_scope_ref_side_02 for an example where this
        # matters.)
        if (set_scope_id := visitor.scopes.get(ir)) is not None:
            set_scope = scope_tree_nodes[set_scope_id]
            for anc, ns in set_scope.ancestors_and_namespaces:
                if anc is scope:
                    path_id = path_id.strip_namespace(ns)
                    break

        visible_paths.add(path_id)

    return visible_paths


def contains_set_of_op(ir: irast.Base) -> bool:
    flt = (lambda n: isinstance(n, irast.Call)
           and any(x == ft.TypeModifier.SetOfType
                   for x in n.params_typemods))
    return bool(ast.find_children(ir, flt, terminate_early=True))
