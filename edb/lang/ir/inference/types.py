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


import collections
import functools
import typing

from edb.lang.common import ast

from edb.lang.schema import basetypes as s_basetypes
from edb.lang.schema import inheriting as s_inh
from edb.lang.schema import name as s_name
from edb.lang.schema import objects as s_obj
from edb.lang.schema import scalars as s_scalars
from edb.lang.schema import types as s_types
from edb.lang.schema import utils as s_utils

from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import errors as ql_errors

from edb.lang.ir import ast as irast


def is_polymorphic_type(t):
    if isinstance(t, s_types.Collection):
        return any(is_polymorphic_type(st) for st in t.get_subtypes())
    else:
        return t.name == 'std::any'


def amend_empty_set_type(es: irast.EmptySet, t: s_obj.Object, schema) -> None:
    alias = es.path_id.target.name.name
    scls_name = s_name.Name(module='__expr__', name=alias)
    scls = t.__class__(name=scls_name, bases=[t])
    scls.acquire_ancestor_inheritance(schema)
    es.path_id = irast.PathId(scls)
    es.scls = t


def _infer_common_type(irs: typing.List[irast.Base], schema):
    if not irs:
        raise ql_errors.EdgeQLError(
            'cannot determine common type of an empty set',
            context=irs[0].context)

    types = []
    empties = []

    seen_object = False
    seen_scalar = False
    seen_coll = False

    for i, arg in enumerate(irs):
        if isinstance(arg, irast.EmptySet) and arg.scls is None:
            empties.append(i)
            continue

        t = infer_type(arg, schema)
        if isinstance(t, s_types.Collection):
            seen_coll = True
        elif isinstance(t, s_scalars.ScalarType):
            seen_scalar = True
        else:
            seen_object = True
        types.append(t)

    if seen_coll + seen_scalar + seen_object > 1:
        raise ql_errors.EdgeQLError(
            'cannot determine common type',
            context=irs[0].context)

    if not types:
        raise ql_errors.EdgeQLError(
            'cannot determine common type of an empty set',
            context=irs[0].context)

    common_type = None
    if seen_scalar or seen_coll:
        it = iter(types)
        common_type = next(it)
        while True:
            next_type = next(it, None)
            if next_type is None:
                break
            common_type = common_type.find_common_implicitly_castable_type(
                next_type, schema)
            if common_type is None:
                break
    else:
        common_type = s_utils.get_class_nearest_common_ancestor(types)

    if common_type is None:
        return None

    for i in empties:
        amend_empty_set_type(irs[i], common_type, schema)

    return common_type


@functools.singledispatch
def _infer_type(ir, schema):
    return


@_infer_type.register(type(None))
def __infer_none(ir, schema):
    # Here for debugging purposes.
    raise ValueError('invalid infer_type(None, schema) call')


@_infer_type.register(irast.Statement)
def __infer_statement(ir, schema):
    return infer_type(ir.expr, schema)


@_infer_type.register(irast.Set)
def __infer_set(ir, schema):
    return ir.scls


@_infer_type.register(irast.FunctionCall)
def __infer_func_call(ir, schema):
    rtype = ir.func.return_type

    if is_polymorphic_type(rtype):
        # Polymorphic function, determine the result type from
        # the argument type.
        if isinstance(rtype, s_types.Tuple):
            for i, arg in enumerate(ir.args):
                if is_polymorphic_type(ir.func.params[i].type):
                    arg_type = infer_type(arg, schema)

                    stypes = collections.OrderedDict(rtype.element_types)
                    for sn, st in stypes.items():
                        if is_polymorphic_type(st):
                            stypes[sn] = arg_type
                            break

                    return rtype.from_subtypes(stypes, rtype.get_typemods())

        elif isinstance(rtype, s_types.Collection):
            for i, arg in enumerate(ir.args):
                if is_polymorphic_type(ir.func.params[i].type):
                    arg_type = infer_type(arg, schema)

                    stypes = list(rtype.get_subtypes())
                    for si, st in enumerate(stypes):
                        if is_polymorphic_type(st):
                            stypes[si] = arg_type
                            break

                    return rtype.from_subtypes(stypes, rtype.get_typemods())

        else:
            for i, arg in enumerate(ir.args):
                if is_polymorphic_type(ir.func.params[i].type):
                    arg_type = infer_type(arg, schema)
                    if isinstance(arg_type, s_types.Collection):
                        stypes = list(arg_type.get_subtypes())
                        return stypes[-1]

    else:
        return rtype


@_infer_type.register(irast.Constant)
@_infer_type.register(irast.Parameter)
def __infer_const_or_param(ir, schema):
    return ir.type


@_infer_type.register(irast.Coalesce)
def __infer_coalesce(ir, schema):
    result = _infer_common_type([ir.left, ir.right], schema)
    if result is None:
        raise ql_errors.EdgeQLError(
            'coalescing operator must have operands of related types',
            context=ir.context)

    return result


@_infer_type.register(irast.SetOp)
def __infer_setop(ir, schema):
    left_type = infer_type(ir.left, schema).material_type()
    right_type = infer_type(ir.right, schema).material_type()

    assert ir.op == qlast.UNION

    if isinstance(left_type, s_scalars.ScalarType):
        result = left_type.find_common_implicitly_castable_type(
            right_type, schema)

    else:
        if left_type.issubclass(right_type):
            result = right_type
        elif right_type.issubclass(left_type):
            result = left_type
        else:
            result = s_inh.create_virtual_parent(
                schema, [left_type, right_type])

    return result


@_infer_type.register(irast.DistinctOp)
def __infer_distinctop(ir, schema):
    result = infer_type(ir.expr, schema)
    return result


def _infer_binop_args(left, right, schema):
    if not isinstance(left, irast.EmptySet) or left.scls is not None:
        left_type = infer_type(left, schema)
    else:
        left_type = None

    if not isinstance(right, irast.EmptySet) or right.scls is not None:
        right_type = infer_type(right, schema)
    else:
        right_type = None

    if left_type is None and right_type is None:
        raise ql_errors.EdgeQLError(
            'cannot determine the type of an empty set',
            context=left.context)
    elif left_type is None:
        amend_empty_set_type(left, right_type, schema)
        left_type = right_type
    elif right_type is None:
        amend_empty_set_type(right, left_type, schema)
        right_type = left_type

    return left_type, right_type


@_infer_type.register(irast.BinOp)
def __infer_binop(ir, schema):
    left_type, right_type = _infer_binop_args(ir.left, ir.right, schema)

    if isinstance(ir.op, (ast.ops.ComparisonOperator,
                          ast.ops.MembershipOperator)):
        result = schema.get('std::bool')
    else:
        result = s_basetypes.TypeRules.get_result(
            ir.op, (left_type, right_type), schema)

        if result is None:
            result = s_basetypes.TypeRules.get_result(
                (ir.op, 'reversed'), (right_type, left_type), schema)

        if result is None:
            if right_type.implicitly_castable_to(left_type, schema):
                right_type = left_type
            elif left_type.implicitly_castable_to(right_type, schema):
                left_type = right_type

            result = s_basetypes.TypeRules.get_result(
                (ir.op, 'reversed'), (right_type, left_type), schema)

    if result is None:
        raise ql_errors.EdgeQLError(
            f'binary operator `{ir.op.upper()}` is not defined for types '
            f'{left_type.name} and {right_type.name}',
            context=ir.left.context)

    return result


@_infer_type.register(irast.EquivalenceOp)
def __infer_equivop(ir, schema):
    left_type, right_type = _infer_binop_args(ir.left, ir.right, schema)
    return schema.get('std::bool')


@_infer_type.register(irast.TypeCheckOp)
def __infer_typecheckop(ir, schema):
    left_type, right_type = _infer_binop_args(ir.left, ir.right, schema)
    return schema.get('std::bool')


@_infer_type.register(irast.UnaryOp)
def __infer_unaryop(ir, schema):
    result = None
    operand_type = infer_type(ir.expr, schema)

    if ir.op == ast.ops.NOT:
        if operand_type.name == 'std::bool':
            result = operand_type

    else:
        if ir.op not in {ast.ops.UPLUS, ast.ops.UMINUS}:
            raise ql_errors.EdgeQLError(
                f'unknown unary operator: {ir.op}',
                context=ir.context)

        result = s_basetypes.TypeRules.get_result(
            ir.op, (operand_type,), schema)

    if result is None:
        raise ql_errors.EdgeQLError(
            f'unary operator `{ir.op.upper()}` is not defined '
            f'for type {operand_type.name}',
            context=ir.context)

    return result


@_infer_type.register(irast.IfElseExpr)
def __infer_ifelse(ir, schema):
    if_expr_type = infer_type(ir.if_expr, schema)
    else_expr_type = infer_type(ir.else_expr, schema)

    result = s_utils.get_class_nearest_common_ancestor(
        [if_expr_type, else_expr_type])

    if result is None:
        raise ql_errors.EdgeQLError(
            'if/else clauses must be of related types, got: {}/{}'.format(
                if_expr_type.name, else_expr_type.name),
            context=ir.if_expr.context)

    return result


@_infer_type.register(irast.TypeRef)
def __infer_typeref(ir, schema):
    if ir.subtypes:
        coll = s_types.Collection.get_class(ir.maintype)
        result = coll.from_subtypes(
            [infer_type(t, schema) for t in ir.subtypes])
    else:
        result = schema.get(ir.maintype)

    return result


@_infer_type.register(irast.TypeCast)
def __infer_typecast(ir, schema):
    return infer_type(ir.type, schema)


@_infer_type.register(irast.Stmt)
def __infer_stmt(ir, schema):
    return infer_type(ir.result, schema)


@_infer_type.register(irast.ExistPred)
def __infer_exist(ir, schema):
    bool_t = schema.get('std::bool')
    if isinstance(ir.expr, irast.EmptySet) and ir.expr.scls is None:
        amend_empty_set_type(ir.expr, bool_t, schema=schema)
    return bool_t


@_infer_type.register(irast.SliceIndirection)
def __infer_slice(ir, schema):
    node_type = infer_type(ir.expr, schema)

    str_t = schema.get('std::str')
    int_t = schema.get('std::int64')
    json_t = schema.get('std::json')

    if node_type.issubclass(str_t):
        base_name = 'string'
    elif node_type.issubclass(json_t):
        base_name = 'json array'
    elif isinstance(node_type, s_types.Array):
        base_name = 'array'
    else:
        # the base type is not valid
        raise ql_errors.EdgeQLError(
            f'cannot index {base_name}',
            context=ir.start.context)

    for index in [ir.start, ir.stop]:
        index_type = infer_type(index, schema)

        if not index_type.implicitly_castable_to(int_t, schema):
            raise ql_errors.EdgeQLError(
                f'cannot index {base_name} by {index_type.name}, '
                f'{int_t.name} was expected',
                context=index.context)

    return node_type


@_infer_type.register(irast.IndexIndirection)
def __infer_index(ir, schema):
    node_type = infer_type(ir.expr, schema)
    index_type = infer_type(ir.index, schema)

    str_t = schema.get('std::str')
    int_t = schema.get('std::int64')
    json_t = schema.get('std::json')

    result = None

    if node_type.issubclass(str_t):

        if not index_type.implicitly_castable_to(int_t, schema):
            raise ql_errors.EdgeQLError(
                f'cannot index string by {index_type.name}, '
                f'{int_t.name} was expected',
                context=ir.index.context)

        result = str_t

    elif node_type.issubclass(json_t):

        if not (index_type.implicitly_castable_to(int_t, schema) or
                index_type.implicitly_castable_to(str_t, schema)):

            raise ql_errors.EdgeQLError(
                f'cannot index json by {index_type.name}, '
                f'{int_t.name} or {str_t.name} was expected',
                context=ir.index.context)

        result = json_t

    elif isinstance(node_type, s_types.Array):

        if not index_type.implicitly_castable_to(int_t, schema):
            raise ql_errors.EdgeQLError(
                f'cannot index array by {index_type.name}, '
                f'{int_t.name} was expected',
                context=ir.index.context)

        result = node_type.element_type

    else:
        raise ql_errors.EdgeQLError(
            f'cannot index {node_type.name}',
            context=ir.index.context)

    return result


@_infer_type.register(irast.Array)
def __infer_array(ir, schema):
    if ir.elements:
        element_type = _infer_common_type(ir.elements, schema)
        if element_type is None:
            raise ql_errors.EdgeQLError('could not determine array type',
                                        context=ir.context)
    else:
        raise ql_errors.EdgeQLError(
            'could not determine type of empty array',
            context=ir.context)

    return s_types.Array(element_type=element_type)


@_infer_type.register(irast.Tuple)
def __infer_struct(ir, schema):
    element_types = {el.name: infer_type(el.val, schema) for el in ir.elements}
    return s_types.Tuple(element_types=element_types, named=ir.named)


@_infer_type.register(irast.TupleIndirection)
def __infer_struct_indirection(ir, schema):
    struct_type = infer_type(ir.expr, schema)
    result = struct_type.element_types.get(ir.name)
    if result is None:
        raise ql_errors.EdgeQLError('could not determine struct element type',
                                    context=ir.context)

    return result


def infer_type(ir, schema):
    try:
        return ir._inferred_type_
    except AttributeError:
        pass

    result = _infer_type(ir, schema)

    if (result is not None and
            not isinstance(result, (s_obj.Object, s_obj.ObjectMeta))):

        raise ql_errors.EdgeQLError(
            f'infer_type({ir!r}) retured {result!r} instead of a Object',
            context=ir.context)

    if result is None or result.name == 'std::any':
        raise ql_errors.EdgeQLError('could not determine expression type',
                                    context=ir.context)

    ir._inferred_type_ = result
    return result
