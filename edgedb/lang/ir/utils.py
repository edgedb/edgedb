##
# Copyright (c) 2015-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import ast

from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import links as s_links
from edgedb.lang.schema import name as s_name
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import views as s_views

from . import ast as irast
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


def extend_path(schema, source_set, ptr):
    scls = source_set.scls

    if isinstance(ptr, str):
        ptrcls = scls.resolve_pointer(schema, ptr)
    else:
        ptrcls = ptr

    path_id = source_set.path_id.extend(
        ptrcls, s_pointers.PointerDirection.Outbound, ptrcls.target)

    target_set = irast.Set()
    target_set.scls = ptrcls.target
    target_set.path_id = path_id

    ptr = irast.Pointer(
        source=source_set,
        target=target_set,
        ptrcls=ptrcls,
        direction=s_pointers.PointerDirection.Outbound
    )

    target_set.rptr = ptr

    return target_set


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
        (isinstance(ir_expr.expr, irast.Stmt) and
            isinstance(ir_expr.expr.result, irast.Set)) or
        ir_expr.view_source is not None
    )


def is_strictly_view_set(ir_expr):
    return (
        isinstance(ir_expr, irast.Set) and
        ir_expr.real_path_id and ir_expr.real_path_id != ir_expr.path_id
    )


def is_subquery_set(ir_expr):
    return (
        isinstance(ir_expr, irast.Set) and
        isinstance(ir_expr.expr, irast.Stmt)
    )


def is_strictly_subquery_set(ir_expr):
    return (
        is_subquery_set(ir_expr) and
        not is_strictly_view_set(ir_expr)
    )


def is_aliased_set(ir_expr):
    return (
        isinstance(ir_expr, irast.Set) and
        len(ir_expr.path_id) == 1 and
        isinstance(ir_expr.path_id[0], s_views.View) and
        ir_expr.path_id[0].name.module == '__aliased__'
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


def get_canonical_set(ir_expr):
    if (isinstance(ir_expr, irast.Set) and ir_expr.source is not None and
            ir_expr.expr is None):
        return ir_expr.source
    else:
        return ir_expr


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


def new_expression_set(ir_expr, schema, path_id=None, alias=None):
    result_type = infer_type(ir_expr, schema)

    if path_id is None:
        if isinstance(ir_expr, irast.TypeFilter):
            type_expr = ir_expr.expr
        else:
            type_expr = ir_expr

        path_id = getattr(type_expr, 'path_id', None)

        if not path_id:
            if alias is None:
                raise ValueError('either path_id or alias are required')
            if isinstance(result_type, (s_concepts.Concept, s_obj.Collection,
                                        s_obj.Tuple)):
                cls = result_type
            else:
                cls_name = s_name.Name(module='__expr__', name=alias)
                cls = result_type.__class__(name=cls_name, bases=[result_type])
                cls.acquire_ancestor_inheritance(schema)
            path_id = irast.PathId([cls])

    return irast.Set(
        path_id=path_id,
        scls=result_type,
        expr=ir_expr,
    )


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


def tuple_indirection_path_id(tuple_path_id, element_name, element_type):
    return tuple_path_id.extend(
        TupleIndirectionLink(element_name),
        s_pointers.PointerDirection.Outbound,
        element_type
    )
