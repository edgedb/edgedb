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
from typing import (
    Any,
    Optional,
    Tuple,
    Type,
    TypeVar,
    AbstractSet,
    Mapping,
    Sequence,
    Dict,
    List,
    Iterable,
    Set,
    cast,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from typing_extensions import TypeGuard

import json
import uuid

from edb import errors

from edb.common import ast
from edb.common import ordered

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

    ir_sets = ast.find_children(
        ir,
        irast.Set,
        lambda n: sub_expr(n) is None or isinstance(n.expr, irast.TypeRoot),
    )
    for ir_set in ir_sets:
        result.add(ir_set)
        if isinstance(ir_set.expr, irast.Pointer):
            parents.add(ir_set.expr.source)

    return result - parents


def get_parameters(ir: irast.Base) -> Set[irast.Parameter]:
    """Return all parameters found in *ir*."""
    return set(ast.find_children(ir, irast.Parameter))


def is_const(ir: irast.Base) -> bool:
    """Return True if the given *ir* expression is constant."""
    roots = ast.find_children(ir, irast.TypeRoot)
    variables = get_parameters(ir)
    return not roots and not variables


def is_union_expr(ir: irast.Base) -> bool:
    """Return True if the given *ir* expression is a UNION expression."""
    return (
        isinstance(ir, irast.OperatorCall) and
        ir.operator_kind is ft.OperatorKind.Infix and
        str(ir.func_shortname) == 'std::UNION'
    )


def is_empty_array_expr(ir: Optional[irast.Base]) -> TypeGuard[irast.Array]:
    """Return True if the given *ir* expression is an empty array expression.
    """
    return (
        isinstance(ir, irast.Array)
        and not ir.elements
    )


def is_untyped_empty_array_expr(
    ir: Optional[irast.Base],
) -> TypeGuard[irast.Array]:
    """Return True if the given *ir* expression is an empty
       array expression of an uknown type.
    """
    return (
        is_empty_array_expr(ir)
        and (ir.typeref is None
             or typeutils.is_generic(ir.typeref))
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
            and is_empty(ir.expr)
        )
    )


def is_subquery_set(ir_expr: irast.Base) -> bool:
    """Return True if the given *ir_expr* expression is a subquery."""
    return (
        isinstance(ir_expr, irast.Set)
        and (
            isinstance(ir_expr.expr, irast.Stmt)
            or (
                isinstance(ir_expr.expr, irast.Pointer)
                and ir_expr.expr.expr is not None
            )
        )
    )


def is_implicit_wrapper(
    ir_expr: Optional[irast.Base],
) -> TypeGuard[irast.SelectStmt]:
    """Return True if the given *ir_expr* expression is an implicit
       SELECT wrapper.
    """
    return (
        isinstance(ir_expr, irast.SelectStmt) and
        ir_expr.implicit_wrapper
    )


def is_trivial_select(ir_expr: irast.Base) -> TypeGuard[irast.SelectStmt]:
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
        and ir_expr.card_inference_override is None
    )


def unwrap_set(ir_set: irast.Set) -> irast.Set:
    """If the given *ir_set* is an implicit SELECT wrapper, return the
       wrapped set.
    """
    if is_implicit_wrapper(ir_set.expr):
        return ir_set.expr.result
    else:
        return ir_set


def get_path_root(ir_set: irast.Set) -> irast.Set:
    result = ir_set
    while isinstance(result.expr, irast.Pointer):
        result = result.expr.source
    return result


def get_span_as_json(
    expr: irast.Base,
    exctype: Type[errors.EdgeDBError] = errors.InternalServerError,
) -> str:
    if expr.span:
        details = json.dumps({
            # TODO(tailhook) should we add offset, utf16column here?
            'line': expr.span.start_point.line,
            'column': expr.span.start_point.column,
            'name': expr.span.name,
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
    if not isinstance(ir_expr.expr, irast.Pointer):
        return False
    rptr = ir_expr.expr

    ir_source = rptr.source

    if ir_source.path_id.is_type_intersection_path():
        source_is_type_intersection = True
    else:
        source_is_type_intersection = False

    return source_is_type_intersection


def is_trivial_free_object(ir: irast.Set) -> bool:
    ir = unwrap_set(ir)
    return (
        isinstance(ir.expr, irast.TypeRoot)
        and typeutils.is_exactly_free_object(ir.typeref)
    )


def collapse_type_intersection(
    ir_set: irast.Set,
) -> Tuple[irast.Set, List[irast.TypeIntersectionPointer]]:

    result: List[irast.TypeIntersectionPointer] = []

    source = ir_set
    while True:
        rptr = source.expr
        if not isinstance(rptr, irast.TypeIntersectionPointer):
            break
        result.append(rptr)
        source = rptr.source

    return source, result


class CollectDMLSourceVisitor(ast.NodeVisitor):
    skip_hidden = True

    def __init__(self) -> None:
        super().__init__()
        self.dml: list[irast.MutatingLikeStmt] = []

    def visit_MutatingLikeStmt(self, stmt: irast.MutatingLikeStmt) -> None:
        # Only INSERTs and UPDATEs produce meaningful overlays.
        if not isinstance(stmt, irast.DeleteStmt):
            self.dml.append(stmt)

    def visit_Set(self, node: irast.Set) -> None:
        # Visit sub-trees
        if node.expr:
            self.visit(node.expr)

    def visit_Pointer(self, node: irast.Pointer) -> None:
        if node.expr:
            self.visit(node.expr)
        else:
            self.visit(node.source)


def get_dml_sources(ir_set: irast.Set) -> Sequence[irast.MutatingLikeStmt]:
    """Find the DML expressions that can contribute to the value of a set

    This is used to compute which overlays to use during SQL compilation.
    """
    # TODO: Make this caching.
    visitor = CollectDMLSourceVisitor()
    visitor.visit(ir_set)
    # Deduplicate, but preserve order. It shouldn't matter for
    # *correctness* but it helps keep the nondeterminism in the output
    # SQL down.
    return tuple(ordered.OrderedSet(visitor.dml))


class ContainsDMLVisitor(ast.NodeVisitor):
    skip_hidden = True

    def __init__(self, *, skip_bindings: bool) -> None:
        super().__init__()
        self.skip_bindings = skip_bindings

    def combine_field_results(self, xs: Iterable[Optional[bool]]) -> bool:
        return any(
            x is True
            or (isinstance(x, (list, tuple)) and self.combine_field_results(x))
            or (isinstance(x, dict) and self.combine_field_results(x.values()))
            for x in xs
        )

    def visit_MutatingStmt(self, stmt: irast.MutatingStmt) -> bool:
        return True

    def visit_Set(self, node: irast.Set) -> bool:
        if self.skip_bindings and node.is_binding:
            return False

        # Visit sub-trees
        return bool(self.generic_visit(node))


def contains_dml(
    stmt: irast.Base,
    *,
    skip_bindings: bool = False,
    skip_nodes: Iterable[irast.Base] = (),
) -> bool:
    """Check whether a statement contains any DML in a subtree."""
    # TODO: Make this caching.
    visitor = ContainsDMLVisitor(skip_bindings=skip_bindings)
    for node in skip_nodes:
        visitor._memo[node] = False
    res = visitor.visit(stmt) is True
    return res


class FindPathScopes(ast.NodeVisitor):
    """Visitor to find the enclosing path scope id of sub expressions.

    Sets inherit an effective scope id from enclosing expressions,
    and this visitor computes those.

    This is set up so that another visitor could inherit from it,
    override process_set, and also collect the scope tree info.
    """

    def __init__(self, init_scope: Optional[int] = None) -> None:
        super().__init__()
        self.path_scope_ids: List[Optional[int]] = [init_scope]
        self.use_scopes: Dict[irast.Set, Optional[int]] = {}
        self.scopes: Dict[irast.Set, Optional[int]] = {}

    def visit_Stmt(self, stmt: irast.Stmt) -> Any:
        # Sometimes there is sharing, so we want the official scope
        # for a node to be based on its appearance in the result,
        # not in a subquery.
        # I think it might not actually matter, though.
        self.visit(stmt.bindings)
        if stmt.iterator_stmt:
            self.visit(stmt.iterator_stmt)
        if isinstance(stmt, (irast.MutatingStmt, irast.GroupStmt)):
            self.visit(stmt.subject)
        if isinstance(stmt, irast.GroupStmt):
            for v in stmt.using.values():
                self.visit(v)
        self.visit(stmt.result)

        return self.generic_visit(stmt)

    def visit_Set(self, node: irast.Set) -> Any:
        val = self.path_scope_ids[-1]
        self.use_scopes[node] = val
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


def find_path_scopes(
    stmt: irast.Base | Sequence[irast.Base],
) -> Dict[irast.Set, Optional[int]]:
    visitor = FindPathScopes()
    visitor.visit(stmt)
    return visitor.scopes


class FindPotentiallyVisibleVisitor(FindPathScopes):
    skip_hidden = True
    extra_skips = frozenset(['materialized_sets'])

    def __init__(
        self,
        to_skip: AbstractSet[irast.PathId],
        scope: irast.ScopeTreeNode,
        scope_tree_nodes: Mapping[int, irast.ScopeTreeNode],
    ) -> None:
        super().__init__(init_scope=scope.unique_id)
        self.to_skip = to_skip
        self.orig_scope = scope
        self.scope_tree_nodes = scope_tree_nodes

    def combine_field_results(self, xs: Any) -> Set[irast.Set]:
        out = set()
        for x in xs:
            if isinstance(x, (list, tuple)):
                x = self.combine_field_results(x)
            if isinstance(x, dict):
                x = self.combine_field_results(x.values())
            if x:
                if isinstance(x, set):
                    out.update(x)
        return out

    def visit_Pointer(self, node: irast.Pointer) -> Set[irast.Set]:
        res: Set[irast.Set] = self.visit(node.source)
        return res

    def process_set(self, node: irast.Set) -> Set[irast.Set]:
        if node.path_id in self.to_skip:
            # We only skip nodes in to_skip if their use site is
            # underneath our original binding site. This prevents us
            # from skipping references to them embedded in outside
            # WITH bindings.
            if (
                (psid := self.use_scopes[node]) is not None
                and (
                    self.orig_scope in
                    self.scope_tree_nodes[psid].ancestors
                )
            ):
                return set()

        results = [{node}]
        if isinstance(node.expr, irast.Pointer):
            results.append(self.visit(node.expr))
            results.append(self.visit(node.shape))
        else:
            results.append(self.visit(node.shape))
            results.append(self.visit(node.expr))

        # Bound variables are always potentially visible as are object
        # references.
        if (
            node.is_binding
            or isinstance(node.expr, irast.TypeRoot)
        ):
            results.append({node})

        # Visit sub-trees
        return self.combine_field_results(results)


def find_potentially_visible(
    stmt: irast.Base,
    scope: irast.ScopeTreeNode,
    scope_tree_nodes: Mapping[int, irast.ScopeTreeNode],
    to_skip: AbstractSet[irast.PathId]=frozenset()
) -> Set[Tuple[irast.PathId, irast.Set]]:
    """Find all "potentially visible" sets referenced."""
    # TODO: Make this caching.
    visitor = FindPotentiallyVisibleVisitor(
        to_skip=to_skip, scope=scope, scope_tree_nodes=scope_tree_nodes)
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

        visible_paths.add((path_id, ir))

    return visible_paths


def is_singleton_set_of_call(
    call: irast.Call
) -> bool:
    # Some set functions and operators are allowed in singleton mode
    # as long as their inputs are singletons

    return bool(call.is_singleton_set_of)


def has_set_of_param(
    call: irast.Call,
) -> bool:
    return any(
        arg.param_typemod == ft.TypeModifier.SetOfType
        for arg in call.args.values()
    )


def returns_set_of(
    call: irast.Call,
) -> bool:
    return call.typemod == ft.TypeModifier.SetOfType


def find_set_of_op(
    ir: irast.Base,
    has_multi_param: bool,
) -> Optional[irast.Call]:
    def flt(n: irast.Call) -> bool:
        return (
            (has_multi_param or not is_singleton_set_of_call(n))
            and (has_set_of_param(n) or returns_set_of(n))
        )
    calls = ast.find_children(ir, irast.Call, flt, terminate_early=True)
    return next(iter(calls or []), None)


ExprT = TypeVar('ExprT', bound=irast.Expr)


def is_set_instance(
    ir: irast.Set,
    typ: Type[ExprT],
) -> TypeGuard[irast.SetE[ExprT]]:
    return isinstance(ir.expr, typ)


def ref_contains_multi(ref: irast.Set, singleton_id: uuid.UUID) -> bool:
    while isinstance(ref.expr, irast.Pointer):
        pointer: irast.Pointer = ref.expr
        if pointer.dir_cardinality.is_multi():
            return True

        # We don't need to look further than the object that we know is a
        # singleton.
        if (
            singleton_id
            and isinstance(pointer.ptrref, irast.PointerRef)
            and pointer.ptrref.id == singleton_id
        ):
            break
        ref = pointer.source
    return False


def sub_expr(ir: irast.Set) -> Optional[irast.Expr]:
    """Fetch the "sub-expression" of a set.

    For a non-pointer Set, it's just the expr, but for a Pointer
    it is the optional computed expression.
    """
    if isinstance(ir.expr, irast.Pointer):
        return ir.expr.expr
    else:
        return ir.expr


class CollectSchemaTypesVisitor(ast.NodeVisitor):
    types: Set[uuid.UUID]

    def __init__(self) -> None:
        super().__init__()
        self.types = set()

    def visit_Set(self, node: irast.Set) -> None:
        self.types.add(node.typeref.id)
        self.generic_visit(node)


def collect_schema_types(stmt: irast.Base) -> Set[uuid.UUID]:
    """Collect ids of all types referenced in the statement."""

    visitor = CollectSchemaTypesVisitor()
    visitor.visit(stmt)
    return visitor.types
