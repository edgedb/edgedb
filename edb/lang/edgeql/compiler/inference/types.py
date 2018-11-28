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


import functools
import typing

from edb.lang.common import ast

from edb.lang.schema import inheriting as s_inh
from edb.lang.schema import name as s_name
from edb.lang.schema import objects as s_obj
from edb.lang.schema import pseudo as s_pseudo
from edb.lang.schema import scalars as s_scalars
from edb.lang.schema import types as s_types
from edb.lang.schema import utils as s_utils

from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import errors as ql_errors

from edb.lang.ir import ast as irast

from .. import context


def amend_empty_set_type(es: irast.EmptySet, t: s_obj.Object, schema) -> None:
    es.stype = t
    alias = es.path_id.target_name.name
    typename = s_name.Name(module='__expr__', name=alias)
    es.path_id = irast.PathId.from_type(schema, t, typename=typename)


def _infer_common_type(irs: typing.List[irast.Base], env):
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
        if isinstance(arg, irast.EmptySet) and arg.stype is None:
            empties.append(i)
            continue

        t = infer_type(arg, env)
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
                next_type, env.schema)
            if common_type is None:
                break
    else:
        common_type = s_utils.get_class_nearest_common_ancestor(
            env.schema, types)

    if common_type is None:
        return None

    for i in empties:
        amend_empty_set_type(irs[i], common_type, env.schema)

    return common_type


@functools.singledispatch
def _infer_type(ir, env):
    return


@_infer_type.register(type(None))
def __infer_none(ir, env):
    # Here for debugging purposes.
    raise ValueError('invalid infer_type(None, env) call')


@_infer_type.register(irast.Statement)
def __infer_statement(ir, env):
    return infer_type(ir.expr, env)


@_infer_type.register(irast.Set)
def __infer_set(ir, env):
    return ir.stype


@_infer_type.register(irast.FunctionCall)
def __infer_func_call(ir, env):
    return ir.stype


@_infer_type.register(irast.BaseConstant)
@_infer_type.register(irast.Parameter)
def __infer_const_or_param(ir, env):
    return ir.stype


@_infer_type.register(irast.Coalesce)
def __infer_coalesce(ir, env):
    result = _infer_common_type([ir.left, ir.right], env)
    if result is None:
        raise ql_errors.EdgeQLError(
            'coalescing operator must have operands of related types',
            context=ir.context)

    return result


@_infer_type.register(irast.SetOp)
def __infer_setop(ir, env):
    left_type = infer_type(ir.left, env).material_type(env.schema)
    right_type = infer_type(ir.right, env).material_type(env.schema)

    assert ir.op == qlast.UNION

    if isinstance(left_type, (s_scalars.ScalarType, s_types.Collection)):
        result = left_type.find_common_implicitly_castable_type(
            right_type, env.schema)

    else:
        if left_type.issubclass(env.schema, right_type):
            result = right_type
        elif right_type.issubclass(env.schema, left_type):
            result = left_type
        else:
            env.schema, result = s_inh.create_virtual_parent(
                env.schema, [left_type, right_type])

    return result


@_infer_type.register(irast.DistinctOp)
def __infer_distinctop(ir, env):
    result = infer_type(ir.expr, env)
    return result


def _infer_binop_args(left, right, env):
    if not isinstance(left, irast.EmptySet) or left.stype is not None:
        left_type = infer_type(left, env)
    else:
        left_type = None

    if not isinstance(right, irast.EmptySet) or right.stype is not None:
        right_type = infer_type(right, env)
    else:
        right_type = None

    if left_type is None and right_type is None:
        raise ql_errors.EdgeQLError(
            'cannot determine the type of an empty set',
            context=left.context)
    elif left_type is None:
        amend_empty_set_type(left, right_type, env.schema)
        left_type = right_type
    elif right_type is None:
        amend_empty_set_type(right, left_type, env.schema)
        right_type = left_type

    return left_type, right_type


@_infer_type.register(irast.BinOp)
def __infer_binop(ir, env):
    left_type, right_type = _infer_binop_args(ir.left, ir.right, env)
    result = None

    if isinstance(ir.op, (ast.ops.ComparisonOperator,
                          ast.ops.MembershipOperator)):
        result = env.schema.get('std::bool')
    else:
        if (isinstance(left_type, s_scalars.ScalarType) and
                isinstance(right_type, s_scalars.ScalarType)):

            if left_type == right_type:
                return left_type

            result = s_scalars.get_op_type(
                ir.op, left_type, right_type, schema=env.schema)

        elif (left_type.is_polymorphic(env.schema) and
                right_type.is_polymorphic(env.schema) and
                left_type == right_type):
            return left_type

    if result is None:
        raise ql_errors.EdgeQLError(
            f'binary operator `{ir.op.upper()}` is not defined for types '
            f'{left_type.get_name(env.schema)} and '
            f'{right_type.get_name(env.schema)}',
            context=ir.left.context)

    return result


@_infer_type.register(irast.EquivalenceOp)
def __infer_equivop(ir, env):
    left_type, right_type = _infer_binop_args(ir.left, ir.right, env)
    return env.schema.get('std::bool')


@_infer_type.register(irast.TypeCheckOp)
def __infer_typecheckop(ir, env):
    left_type, right_type = _infer_binop_args(ir.left, ir.right, env)
    return env.schema.get('std::bool')


@_infer_type.register(irast.UnaryOp)
def __infer_unaryop(ir, env):
    result = None
    operand_type = infer_type(ir.expr, env)

    if isinstance(operand_type, s_scalars.ScalarType):
        result = s_scalars.get_op_type(
            ir.op, operand_type, schema=env.schema)

    if result is None:
        raise ql_errors.EdgeQLError(
            f'unary operator `{ir.op.upper()}` is not defined '
            f'for type {operand_type.get_name(env.schema)}',
            context=ir.context)

    return result


@_infer_type.register(irast.IfElseExpr)
def __infer_ifelse(ir, env):
    result = _infer_common_type(
        [ir.if_expr, ir.else_expr], env)

    if result is None:
        if_expr_type = infer_type(ir.if_expr, env)
        else_expr_type = infer_type(ir.else_expr, env)
        raise ql_errors.EdgeQLError(
            'if/else clauses must be of related types, got: {}/{}'.format(
                if_expr_type.get_name(env.schema),
                else_expr_type.get_name(env.schema)),
            context=ir.if_expr.context)

    return result


@_infer_type.register(irast.TypeRef)
def __infer_typeref(ir, env):
    if ir.subtypes:
        coll = s_types.Collection.get_class(ir.maintype)
        result = coll.from_subtypes(
            env.schema, [infer_type(t, env) for t in ir.subtypes])
    else:
        result = env.schema.get(ir.maintype)

    return result


@_infer_type.register(irast.TypeCast)
def __infer_typecast(ir, env):
    stype = infer_type(ir.type, env)

    # is_polymorphic is synonymous to get_is_abstract for scalars
    if stype.is_polymorphic(env.schema):
        raise ql_errors.EdgeQLError(
            f'cannot cast into an abstract scalar '
            f'{stype.get_displayname(env.schema)}',
            context=ir.context)

    return stype


@_infer_type.register(irast.Stmt)
def __infer_stmt(ir, env):
    return infer_type(ir.result, env)


@_infer_type.register(irast.ExistPred)
def __infer_exist(ir, env):
    bool_t = env.schema.get('std::bool')
    if isinstance(ir.expr, irast.EmptySet) and ir.expr.stype is None:
        amend_empty_set_type(ir.expr, bool_t, schema=env.schema)
    return bool_t


@_infer_type.register(irast.SliceIndirection)
def __infer_slice(ir, env):
    node_type = infer_type(ir.expr, env)

    str_t = env.schema.get('std::str')
    int_t = env.schema.get('std::int64')
    json_t = env.schema.get('std::json')
    bytes_t = env.schema.get('std::bytes')

    if node_type.issubclass(env.schema, str_t):
        base_name = 'string'
    elif node_type.issubclass(env.schema, json_t):
        base_name = 'json array'
    elif node_type.issubclass(env.schema, bytes_t):
        base_name = 'bytes'
    elif isinstance(node_type, s_types.Array):
        base_name = 'array'
    elif node_type.is_any():
        base_name = 'anytype'
    else:
        # the base type is not valid
        raise ql_errors.EdgeQLError(
            f'cannot slice {node_type.get_name(env.schema)}',
            context=ir.start.context)

    for index in [ir.start, ir.stop]:
        if index is not None:
            index_type = infer_type(index, env)

            if not index_type.implicitly_castable_to(int_t, env.schema):
                raise ql_errors.EdgeQLError(
                    f'cannot slice {base_name} by '
                    f'{index_type.get_name(env.schema)}, '
                    f'{int_t.get_name(env.schema)} was expected',
                    context=index.context)

    return node_type


@_infer_type.register(irast.IndexIndirection)
def __infer_index(ir, env):
    node_type = infer_type(ir.expr, env)
    index_type = infer_type(ir.index, env)

    str_t = env.schema.get('std::str')
    bytes_t = env.schema.get('std::bytes')
    int_t = env.schema.get('std::int64')
    json_t = env.schema.get('std::json')

    result = None

    if node_type.issubclass(env.schema, str_t):

        if not index_type.implicitly_castable_to(int_t, env.schema):
            raise ql_errors.EdgeQLError(
                f'cannot index string by {index_type.get_name(env.schema)}, '
                f'{int_t.get_name(env.schema)} was expected',
                context=ir.index.context)

        result = str_t

    elif node_type.issubclass(env.schema, bytes_t):

        if not index_type.implicitly_castable_to(int_t, env.schema):
            raise ql_errors.EdgeQLError(
                f'cannot index bytes by {index_type.get_name(env.schema)}, '
                f'{int_t.get_name(env.schema)} was expected',
                context=ir.index.context)

        result = bytes_t

    elif node_type.issubclass(env.schema, json_t):

        if not (index_type.implicitly_castable_to(int_t, env.schema) or
                index_type.implicitly_castable_to(str_t, env.schema)):

            raise ql_errors.EdgeQLError(
                f'cannot index json by {index_type.get_name(env.schema)}, '
                f'{int_t.get_name(env.schema)} or '
                f'{str_t.get_name(env.schema)} was expected',
                context=ir.index.context)

        result = json_t

    elif isinstance(node_type, s_types.Array):

        if not index_type.implicitly_castable_to(int_t, env.schema):
            raise ql_errors.EdgeQLError(
                f'cannot index array by {index_type.get_name(env.schema)}, '
                f'{int_t.get_name(env.schema)} was expected',
                context=ir.index.context)

        result = node_type.element_type

    elif (node_type.is_any() or
            (node_type.is_scalar() and
                node_type.get_name(env.schema) == 'std::anyscalar') and
            (index_type.implicitly_castable_to(int_t, env.schema) or
                index_type.implicitly_castable_to(str_t, env.schema))):
        result = s_pseudo.Any.create()

    else:
        raise ql_errors.EdgeQLError(
            f'cannot index {node_type.get_name(env.schema)}',
            context=ir.index.context)

    return result


@_infer_type.register(irast.Array)
def __infer_array(ir, env):
    if ir.elements:
        element_type = _infer_common_type(ir.elements, env)
        if element_type is None:
            raise ql_errors.EdgeQLError('could not determine array type',
                                        context=ir.context)
    else:
        raise ql_errors.EdgeQLError(
            'could not determine type of empty array',
            context=ir.context)

    return s_types.Array.create(env.schema, element_type=element_type)


@_infer_type.register(irast.Tuple)
def __infer_struct(ir, env):
    element_types = {el.name: infer_type(el.val, env) for el in ir.elements}
    return s_types.Tuple.create(
        env.schema, element_types=element_types, named=ir.named)


@_infer_type.register(irast.TupleIndirection)
def __infer_struct_indirection(ir, env):
    struct_type = infer_type(ir.expr, env)
    result = struct_type.get_subtype(env.schema, ir.name)
    if result is None:
        raise ql_errors.EdgeQLError('could not determine struct element type',
                                    context=ir.context)

    return result


def infer_type(ir: irast.Base, env: context.Environment):
    try:
        return ir._inferred_type_
    except AttributeError:
        pass

    result = _infer_type(ir, env)

    if (result is not None and
            not isinstance(result, (s_obj.Object, s_obj.ObjectMeta))):

        raise ql_errors.EdgeQLError(
            f'infer_type({ir!r}) retured {result!r} instead of a Object',
            context=ir.context)

    if result is None:
        raise ql_errors.EdgeQLError('could not determine expression type',
                                    context=ir.context)

    ir._inferred_type_ = result
    return result
