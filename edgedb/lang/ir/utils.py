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


from edgedb.lang.common import ast

from edgedb.lang.schema import objtypes as s_objtypes
from edgedb.lang.schema import links as s_links
from edgedb.lang.schema import name as s_name
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import schema as s_schema
from edgedb.lang.schema import sources as s_sources  # NOQA

from . import ast as irast
from .inference import amend_empty_set_type  # NOQA
from .inference import infer_type  # NOQA
from .inference import is_polymorphic_type  # NOQA


def get_source_references(ir):
    result = set()

    flt = lambda n: isinstance(n, irast.Set) and n.expr is None
    ir_sets = ast.find_children(ir, flt)
    for ir_set in ir_sets:
        result.add(ir_set.scls)

    return result


def get_terminal_references(ir):
    result = set()
    parents = set()

    flt = lambda n: isinstance(n, irast.Set) and n.expr is None
    ir_sets = ast.find_children(ir, flt)
    for ir_set in ir_sets:
        result.add(ir_set)
        if ir_set.rptr:
            parents.add(ir_set.rptr.source)

    return result - parents


def get_variables(ir):
    result = set()
    flt = lambda n: isinstance(n, irast.Parameter)
    result.update(ast.find_children(ir, flt))
    return result


def is_const(ir):
    flt = lambda n: isinstance(n, irast.Set) and n.expr is None
    ir_sets = ast.find_children(ir, flt)
    variables = get_variables(ir)
    return not ir_sets and not variables


def is_set_membership_expr(ir):
    return (
        isinstance(ir, irast.BinOp) and
        isinstance(ir.op, ast.ops.MembershipOperator)
    )


def is_aggregated_expr(ir):
    def flt(n):
        if isinstance(n, irast.FunctionCall):
            return n.func.aggregate
        elif isinstance(n, irast.Stmt):
            # Make sure we don't dip into subqueries
            raise ast.SkipNode()

    return bool(set(ast.find_children(ir, flt)))


def get_id_path_id(
        path_id: irast.PathId, *,
        schema: s_schema.Schema) -> irast.PathId:
    """For PathId representing an object, return (PathId).(std::id)."""
    source: s_sources.Source = path_id[-1]
    assert isinstance(source, s_objtypes.ObjectType)
    return path_id.extend(
        source.resolve_pointer(schema, 'std::id'),
        s_pointers.PointerDirection.Outbound,
        schema.get('std::uuid'))


def get_subquery_shape(ir_expr):
    if (isinstance(ir_expr, irast.Set) and
            isinstance(ir_expr.expr, irast.Stmt) and
            isinstance(ir_expr.expr.result, irast.Set)):
        result = ir_expr.expr.result
        if result.shape:
            return result
        elif is_view_set(result):
            return get_subquery_shape(result)
    elif ir_expr.view_source is not None:
        return get_subquery_shape(ir_expr.view_source)
    else:
        return None


def is_view_set(ir_expr):
    return (
        isinstance(ir_expr, irast.Set) and
        (isinstance(ir_expr.expr, irast.SelectStmt) and
            isinstance(ir_expr.expr.result, irast.Set)) or
        ir_expr.view_source is not None
    )


def is_subquery_set(ir_expr):
    return (
        isinstance(ir_expr, irast.Set) and
        isinstance(ir_expr.expr, irast.Stmt)
    )


def is_scalar_view_set(ir_expr):
    return (
        isinstance(ir_expr, irast.Set) and
        len(ir_expr.path_id) == 1 and
        ir_expr.path_id.is_scalar_path() and
        ir_expr.path_id[0].is_view()
    )


def is_inner_view_reference(ir_expr):
    return (
        isinstance(ir_expr, irast.Set) and
        ir_expr.view_source is not None
    )


def is_simple_path(ir_expr):
    return (
        isinstance(ir_expr, irast.Set) and
        ir_expr.expr is None and
        (ir_expr.rptr is None or is_simple_path(ir_expr.rptr.source))
    )


def wrap_stmt_set(ir_set):
    if is_subquery_set(ir_set):
        src_stmt = ir_set.expr
    elif is_inner_view_reference(ir_set):
        src_stmt = ir_set.view_source.expr
    else:
        raise ValueError('expecting subquery IR set or a view reference')

    stmt = irast.SelectStmt(
        result=ir_set,
        path_scope=src_stmt.path_scope,
        specific_path_scope=src_stmt.specific_path_scope
    )
    return stmt


def is_simple_wrapper(ir_expr):
    if not isinstance(ir_expr, irast.SelectStmt):
        return False

    return (
        isinstance(ir_expr.result, irast.Stmt) or
        is_subquery_set(ir_expr.result)
    )


def new_empty_set(schema, *, scls=None, alias):
    if scls is None:
        base_scls = schema.get('std::str')
    else:
        base_scls = scls
    cls_name = s_name.Name(module='__expr__', name=alias)
    cls = base_scls.__class__(name=cls_name, bases=[base_scls])
    cls.acquire_ancestor_inheritance(schema)
    return irast.EmptySet(path_id=irast.PathId(cls), scls=scls)


class TupleIndirectionLink(s_links.Link):
    """A Link subclass that can be used in tuple indirection path ids."""

    def __init__(self, element_name):
        super().__init__(
            name=s_name.Name(module='__tuple__', name=str(element_name))
        )

    def __hash__(self):
        return hash((self.__class__, self.name))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.name == other.name

    def generic(self):
        # Make PathId happy.
        return False


def tuple_indirection_path_id(tuple_path_id, element_name, element_type):
    return tuple_path_id.extend(
        TupleIndirectionLink(element_name),
        s_pointers.PointerDirection.Outbound,
        element_type
    )


class TypeIndirectionLink(s_links.Link):
    """A Link subclass that can be used in type indirection path ids."""

    def __init__(self, source, target, *, optional, cardinality):
        name = 'optindirection' if optional else 'indirection'
        super().__init__(
            name=s_name.Name(module='__type__', name=name),
            source=source,
            target=target,
            direction=s_pointers.PointerDirection.Outbound
        )
        self.optional = optional
        self.cardinality = cardinality

    def __hash__(self):
        return hash((self.__class__, self.name, self.source, self.target))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return (self.name == other.name and self.source == other.source and
                self.target == other.target)

    def generic(self):
        # Make PathId happy.
        return False


def type_indirection_path_id(path_id, target_type, *, optional: bool,
                             cardinality: s_pointers.PointerCardinality):
    return path_id.extend(
        TypeIndirectionLink(path_id[-1], target_type,
                            optional=optional, cardinality=cardinality),
        s_pointers.PointerDirection.Outbound,
        target_type
    )
