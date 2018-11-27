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


import json
import typing

from edb import errors

from edb.lang.common import ast

from edb.lang.edgeql import functypes as ft

from edb.lang.schema import abc as s_abc
from edb.lang.schema import objtypes as s_objtypes
from edb.lang.schema import name as s_name
from edb.lang.schema import pointers as s_pointers
from edb.lang.schema import pseudo as s_pseudo
from edb.lang.schema import schema as s_schema
from edb.lang.schema import sources as s_sources  # NOQA
from edb.lang.schema import types as s_types  # NOQA

from . import ast as irast


def get_source_references(ir):
    result = set()

    flt = lambda n: isinstance(n, irast.Set) and n.expr is None
    ir_sets = ast.find_children(ir, flt)
    for ir_set in ir_sets:
        result.add(ir_set.stype)

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
        isinstance(ir, irast.OperatorCall) and
        ir.operator_kind is ft.OperatorKind.INFIX and
        ir.func_shortname in {'std::IN', 'std::NOT IN'}
    )


def is_distinct_expr(ir):
    return (
        isinstance(ir, irast.OperatorCall) and
        ir.operator_kind is ft.OperatorKind.PREFIX and
        ir.func_shortname == 'std::DISTINCT'
    )


def is_exists_expr(ir):
    return (
        isinstance(ir, irast.OperatorCall) and
        ir.operator_kind is ft.OperatorKind.PREFIX and
        ir.func_shortname == 'std::EXISTS'
    )


def is_empty_array_expr(ir):
    return (
        isinstance(ir, irast.Array)
        and not ir.elements
    )


def is_untyped_empty_array_expr(ir):
    return (
        is_empty_array_expr(ir)
        and (ir.stype is None or ir.stype.contains_any())
    )


def get_id_path_id(
        path_id: irast.PathId, *,
        schema: s_schema.Schema) -> irast.PathId:
    """For PathId representing an object, return (PathId).(std::id)."""
    source: s_sources.Source = path_id.target
    assert isinstance(source, s_objtypes.ObjectType)
    return path_id.extend(
        source.getptr(schema, 'id'),
        s_pointers.PointerDirection.Outbound,
        schema.get('std::uuid'),
        schema=schema)


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


def is_empty(ir_expr):
    return (
        isinstance(ir_expr, irast.EmptySet) or
        (isinstance(ir_expr, irast.Array) and not ir_expr.elements) or
        (isinstance(ir_expr, irast.Set) and is_empty(ir_expr.expr))
    )


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


def is_scalar_view_set(ir_expr, *, schema: s_schema.Schema):
    return (
        isinstance(ir_expr, irast.Set) and
        len(ir_expr.path_id) == 1 and
        ir_expr.path_id.is_scalar_path() and
        ir_expr.path_id.target.is_view(schema)
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


def is_implicit_wrapper(ir_expr):
    return (
        isinstance(ir_expr, irast.SelectStmt) and
        ir_expr.implicit_wrapper
    )


def unwrap_set(ir_set):
    if is_implicit_wrapper(ir_set.expr):
        return ir_set.expr.result
    else:
        return ir_set


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


def new_empty_set(schema, *, stype=None, alias):
    if stype is None:
        path_id_scls = s_pseudo.Any.create()
    else:
        path_id_scls = stype

    typename = s_name.Name(module='__expr__', name=alias)
    path_id = irast.PathId.from_type(schema, path_id_scls, typename=typename)
    return irast.EmptySet(path_id=path_id, stype=stype)


class TupleIndirectionLink(s_pointers.PointerLike):
    """A Link-alike that can be used in tuple indirection path ids."""

    def __init__(self, element_name):
        self._name = s_name.Name(module='__tuple__', name=str(element_name))

    def __hash__(self):
        return hash((self.__class__, self._name))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self._name == other._name

    def get_shortname(self, schema):
        return self._name

    def get_name(self, schema):
        return self._name

    def get_path_id_name(self, schema):
        return self._name

    def is_link_property(self, schema):
        return False

    def generic(self, schema):
        return False

    def get_source(self, schema):
        return None

    def singular(self, schema,
                 direction=s_pointers.PointerDirection.Outbound) -> bool:
        return True

    def scalar(self):
        return self._target.is_scalar()

    def is_pure_computable(self, schema):
        return False


def tuple_indirection_path_id(tuple_path_id, element_name, element_type, *,
                              schema):
    return tuple_path_id.extend(
        TupleIndirectionLink(element_name),
        s_pointers.PointerDirection.Outbound,
        element_type,
        schema=schema
    )


class TypeIndirectionLink(s_pointers.PointerLike):
    """A Link-alike that can be used in type indirection path ids."""

    def __init__(self, source, target, *, optional, cardinality):
        name = 'optindirection' if optional else 'indirection'
        self._name = s_name.Name(module='__type__', name=name)
        self._source = source
        self._target = target
        self._cardinality = cardinality
        self._optional = optional

    def get_name(self, schema):
        return self._name

    def get_shortname(self, schema):
        return self._name

    def get_path_id_name(self, schema):
        return self._name

    def is_link_property(self, schema):
        return False

    def generic(self, schema):
        return False

    def get_source(self, schema):
        return self._source

    def get_target(self, schema):
        return self._target

    def get_cardinality(self, schema):
        return self._cardinality

    def singular(self, schema,
                 direction=s_pointers.PointerDirection.Outbound) -> bool:
        if direction is s_pointers.PointerDirection.Outbound:
            return self.get_cardinality(schema) is irast.Cardinality.ONE
        else:
            return True

    def scalar(self):
        return self._target.is_scalar()

    def is_pure_computable(self, schema):
        return False


def type_indirection_path_id(path_id, target_type, *, optional: bool,
                             cardinality: irast.Cardinality,
                             schema):
    return path_id.extend(
        TypeIndirectionLink(path_id.target, target_type,
                            optional=optional, cardinality=cardinality),
        s_pointers.PointerDirection.Outbound,
        target_type,
        schema=schema
    )


def get_source_context_as_json(
        expr: irast.Base,
        exctype=errors.InternalServerError) -> typing.Optional[str]:
    if expr.context:
        details = json.dumps({
            'line': expr.context.start.line,
            'column': expr.context.start.column,
            'name': expr.context.name,
            'code': exctype.get_code(),
        })

    else:
        details = None

    return details


def typeref_to_type(schema, typeref: irast.TypeRef) -> s_types.Type:

    if typeref.subtypes:
        coll = s_types.Collection.get_class(typeref.maintype)
        result = coll.from_subtypes(
            schema, [typeref_to_type(schema, t) for t in typeref.subtypes])
    else:
        result = schema.get(typeref.maintype)

    return result


def type_to_typeref(schema, t: s_types.Type, *, _name=None) -> irast.TypeRef:

    if t.is_anytuple():
        result = irast.AnyTupleRef()
    elif t.is_any():
        result = irast.AnyTypeRef()
    elif not isinstance(t, s_abc.Collection):
        result = irast.TypeRef(
            name=_name,
            maintype=t.get_name(schema),
        )
    elif isinstance(t, s_abc.Tuple) and t.named:
        result = irast.TypeRef(
            name=_name,
            maintype=t.schema_name,
            subtypes=[
                type_to_typeref(schema, st, _name=sn)
                for sn, st in t.element_types.items()
            ]
        )
    else:
        result = irast.TypeRef(
            name=_name,
            maintype=t.schema_name,
            subtypes=[
                type_to_typeref(schema, st)
                for st in t.get_subtypes()
            ]
        )

    return result
