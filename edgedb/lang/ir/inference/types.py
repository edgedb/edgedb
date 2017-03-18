##
# Copyright (c) 2015-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import functools
import typing

from edgedb.lang.common import ast

from edgedb.lang.schema import inheriting as s_inh
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import types as s_types
from edgedb.lang.schema import utils as s_utils

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import errors as ql_errors

from edgedb.lang.ir import ast as irast


def infer_arg_types(ir, schema):
    def flt(n):
        if isinstance(n, irast.BinOp):
            return (isinstance(n.left, irast.Parameter) or
                    isinstance(n.right, irast.Parameter))

    ops = ast.find_children(ir, flt)

    arg_types = {}

    for binop in ops:
        typ = None

        if isinstance(binop.right, irast.Parameter):
            expr = binop.left
            arg = binop.right
            reversed = False
        else:
            expr = binop.right
            arg = binop.left
            reversed = True

        if isinstance(binop.op, irast.EdgeDBMatchOperator):
            typ = schema.get('std::str')

        elif isinstance(binop.op, (ast.ops.ComparisonOperator,
                                   ast.ops.ArithmeticOperator)):
            typ = infer_type(expr, schema)

        elif isinstance(binop.op, ast.ops.MembershipOperator) and not reversed:
            from edgedb.lang.schema import objects as s_obj

            elem_type = infer_type(expr, schema)
            typ = s_obj.Set(element_type=elem_type)

        elif isinstance(binop.op, ast.ops.BooleanOperator):
            typ = schema.get('std::bool')

        else:
            msg = 'cannot infer expr type: unsupported ' \
                  'operator: {!r}'.format(binop.op)
            raise ValueError(msg)

        if typ is None:
            msg = 'cannot infer expr type'
            raise ValueError(msg)

        try:
            existing = arg_types[arg.name]
        except KeyError:
            arg_types[arg.name] = typ
        else:
            if existing != typ:
                msg = 'cannot infer expr type: ambiguous resolution: ' + \
                      '{!r} and {!r}'
                raise ValueError(msg.format(existing, typ))

    return arg_types


def _infer_common_type(irs: typing.List[irast.Base], schema):
    if not irs:
        raise ql_errors.EdgeQLError(
            'cannot determine common type of an empty set',
            context=irs[0].context)

    col_type = None
    arg_types = []
    for arg in irs:
        if isinstance(arg, irast.EmptySet):
            continue

        arg_type = infer_type(arg, schema)
        arg_types.append(arg_type)

        if isinstance(arg_type, s_obj.Collection):
            col_type = arg_type

    if not arg_types:
        raise ql_errors.EdgeQLError(
            'cannot determine common type of an empty set',
            context=irs[0].context)

    if col_type is not None:
        if not all(col_type.issubclass(t) for t in arg_types):
            raise ql_errors.EdgeQLError(
                'cannot determine common type',
                context=irs[0].context)
        return col_type
    else:
        return s_utils.get_class_nearest_common_ancestor(arg_types)


@functools.singledispatch
def _infer_type(ir, schema):
    return


@_infer_type.register(type(None))
def __infer_none(ir, schema):
    # Here for debugging purposes.
    raise ValueError('invalid infer_type(None, schema) call')


@_infer_type.register(irast.Set)
def __infer_set_or_shape(ir, schema):
    return ir.scls


@_infer_type.register(irast.FunctionCall)
def __infer_func_call(ir, schema):
    rtype = ir.func.returntype
    result = rtype

    def is_polymorphic(t):
        if isinstance(t, s_obj.Collection):
            t = t.get_element_type()

        return t.name == 'std::any'

    if is_polymorphic(result):
        # Polymorhic function, determine the result type from
        # the argument type.
        for i, arg in enumerate(ir.args):
            if is_polymorphic(ir.func.paramtypes[i]):
                result = infer_type(arg, schema)
                if isinstance(rtype, s_obj.Collection):
                    stypes = list(rtype.get_subtypes())
                    stypes[-1] = result
                    result = rtype.from_subtypes(stypes)
                break

    return result


@_infer_type.register(irast.Constant)
@_infer_type.register(irast.Parameter)
def __infer_const_or_param(ir, schema):
    return ir.type


@_infer_type.register(irast.Coalesce)
def __infer_coalesce(ir, schema):
    result = _infer_common_type(ir.args, schema)
    if result is None:
        raise ql_errors.EdgeQLError(
            'coalescing operator must have operands of related types',
            context=ir.context)

    return result


@_infer_type.register(irast.SetOp)
def __infer_setop(ir, schema):
    left_type = infer_type(ir.left, schema)
    right_type = infer_type(ir.right, schema)

    if ir.op == qlast.UNION:
        if left_type.issubclass(right_type):
            result = left_type
        elif right_type.issubclass(left_type):
            result = right_type
        else:
            result = s_inh.create_virtual_parent(
                schema, [left_type, right_type])

    else:
        result = infer_type(ir.left, schema)
        # create_virtual_parent will raise if types are incompatible.
        s_inh.create_virtual_parent(schema, [left_type, right_type])

    return result


@_infer_type.register(irast.BinOp)
def __infer_binop(ir, schema):
    if isinstance(ir.op, (ast.ops.ComparisonOperator,
                          ast.ops.TypeCheckOperator,
                          ast.ops.MembershipOperator,
                          irast.EquivalenceOperator,
                          irast.TextSearchOperator)):
        return schema.get('std::bool')

    if isinstance(ir.left, irast.EmptySet):
        return infer_type(ir.right, schema)
    elif isinstance(ir.right, irast.EmptySet):
        return infer_type(ir.left, schema)

    left_type = infer_type(ir.left, schema)
    right_type = infer_type(ir.right, schema)

    result = s_types.TypeRules.get_result(
        ir.op, (left_type, right_type), schema)
    if result is None:
        result = s_types.TypeRules.get_result(
            (ir.op, 'reversed'), (right_type, left_type), schema)

    if result is None:
        raise ql_errors.EdgeQLError(
            f'binary operator `{ir.op.upper()}` is not defined for types '
            f'{left_type.name} and {right_type.name}',
            context=ir.left.context)

    return result


@_infer_type.register(irast.UnaryOp)
def __infer_unaryop(ir, schema):
    result = None

    if isinstance(ir.expr, irast.EmptySet):
        raise ql_errors.EdgeQLError(
            f'unary operator `{ir.op.upper()}` is not defined for empty set',
            context=ir.context)

    operand_type = infer_type(ir.expr, schema)

    if ir.op == ast.ops.NOT:
        if operand_type.name == 'std::bool':
            result = operand_type

    else:
        if ir.op not in {ast.ops.UPLUS, ast.ops.UMINUS}:
            raise ql_errors.EdgeQLError(
                f'unknown unary operator: {ir.op}',
                context=ir.context)

        result = s_types.TypeRules.get_result(
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


@_infer_type.register(irast.TypeCast)
@_infer_type.register(irast.TypeFilter)
def __infer_typecast(ir, schema):
    if ir.type.subtypes:
        coll = s_obj.Collection.get_class(ir.type.maintype)
        result = coll.from_subtypes(
            [schema.get(t) for t in ir.type.subtypes])
    else:
        result = schema.get(ir.type.maintype)
    return result


@_infer_type.register(irast.Stmt)
def __infer_stmt(ir, schema):
    return infer_type(ir.result, schema)


@_infer_type.register(irast.ExistPred)
def __infer_exist(ir, schema):
    return schema.get('std::bool')


@_infer_type.register(irast.SliceIndirection)
def __infer_slice(ir, schema):
    return infer_type(ir.expr, schema)


@_infer_type.register(irast.IndexIndirection)
def __infer_index(ir, schema):
    node_type = infer_type(ir.expr, schema)
    index_type = infer_type(ir.index, schema)

    str_t = schema.get('std::str')
    int_t = schema.get('std::int')

    result = None

    if node_type.issubclass(str_t):

        if not index_type.issubclass(int_t):
            raise ql_errors.EdgeQLError(
                f'cannot index string by {index_type.name}, '
                f'{int_t.name} was expected',
                context=ir.index.context)

        result = str_t

    elif isinstance(node_type, s_obj.Map):

        if not index_type.issubclass(node_type.key_type):
            raise ql_errors.EdgeQLError(
                f'cannot index {node_type.name} by {index_type.name}, '
                f'{node_type.key_type.name} was expected',
                context=ir.index.context)

        result = node_type.element_type

    elif isinstance(node_type, s_obj.Array):

        if not index_type.issubclass(int_t):
            raise ql_errors.EdgeQLError(
                f'cannot index array by {index_type.name}, '
                f'{int_t.name} was expected',
                context=ir.index.context)

        result = node_type.element_type

    return result


@_infer_type.register(irast.Mapping)
def __infer_map(ir, schema):
    if not ir.keys:
        raise ql_errors.EdgeQLError('could not determine type of empty map',
                                    context=ir.context)

    key_type = _infer_common_type(ir.keys, schema)
    if key_type is None:
        raise ql_errors.EdgeQLError('could not determine map keys type',
                                    context=ir.context)

    element_type = _infer_common_type(ir.values, schema)
    if element_type is None:
        raise ql_errors.EdgeQLError('could not determine map values type',
                                    context=ir.context)

    return s_obj.Map(key_type=key_type, element_type=element_type)


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

    return s_obj.Array(element_type=element_type)


@_infer_type.register(irast.Sequence)
def __infer_tuple(ir, schema):
    return s_obj.Tuple(element_type=schema.get('std::any'))


@_infer_type.register(irast.Struct)
def __infer_struct(ir, schema):
    element_types = {el.name: infer_type(el.val, schema) for el in ir.elements}
    return s_obj.Struct(
        element_type=schema.get('std::any'),
        element_types=element_types)


@_infer_type.register(irast.StructIndirection)
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
            not isinstance(result, (s_obj.Class, s_obj.MetaClass))):

        raise ql_errors.EdgeQLError(
            f'infer_type({ir!r}) retured {result!r} instead of a Class',
            context=ir.context)

    if result is None or result.name == 'std::any':
        raise ql_errors.EdgeQLError('could not determine expression type',
                                    context=ir.context)

    ir._inferred_type_ = result
    return result
