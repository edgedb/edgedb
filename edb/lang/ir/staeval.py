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


"""Static evaluation of EdgeQL IR."""


import decimal
import functools

from edb.lang.common import ast
from edb.lang.edgeql import errors
from edb.lang.edgeql import lexutils as ql_lexutils

from edb.lang.ir import ast as irast
from edb.lang.ir import utils as irutils

from edb.lang.schema import schema as s_schema
from edb.lang.schema import scalars as s_scalars


class StaticEvaluationError(errors.EdgeQLError):
    pass


class UnsupportedExpressionError(errors.EdgeQLError):
    pass


def evaluate_to_python_val(
        ir: irast.Base,
        schema: s_schema.Schema) -> object:
    const = evaluate(ir, schema=schema)
    return const_to_python(const, schema=schema)


@functools.singledispatch
def evaluate(ir: irast.Base, schema: s_schema.Schema) -> irast.BaseConstant:
    raise UnsupportedExpressionError(
        f'no static IR evaluation handler for {ir.__class__}')


@evaluate.register(irast.Set)
def evaluate_Set(
        ir_set: irast.Set,
        schema: s_schema.Schema) -> irast.BaseConstant:
    if ir_set.expr is not None:
        return evaluate(ir_set.expr, schema=schema)
    else:
        raise UnsupportedExpressionError(
            'expression is not constant', context=ir_set.context)


@evaluate.register(irast.BaseConstant)
def evaluate_BaseConstant(
        ir_const: irast.BaseConstant,
        schema: s_schema.Schema) -> irast.BaseConstant:
    return ir_const


@evaluate.register(irast.BinOp)
def evaluate_BinOp(
        binop: irast.BinOp,
        schema: s_schema.Schema) -> irast.BaseConstant:

    real_t = schema.get('std::anyreal')

    result_type = irutils.infer_type(binop, schema)
    folded = None
    op = binop.op

    # Left and right nodes are constants.
    if isinstance(op, ast.ops.ComparisonOperator):
        folded = evaluate_comparison_binop(binop, schema=schema)

    elif result_type.issubclass(real_t):
        folded = evaluate_arithmetic_binop(binop, schema=schema)

    else:
        raise UnsupportedExpressionError(
            'expression is not constant', context=binop.context)

    return folded


@evaluate.register(irast.UnaryOp)
def evaluate_UnaryOp(
        unop: irast.UnaryOp,
        schema: s_schema.Schema) -> irast.BaseConstant:

    real_t = schema.get('std::anyreal')
    int_t = schema.get('std::anyint')

    result_type = unop.expr.scls

    if (result_type.issubclass(real_t) and
            unop.op in (ast.ops.UMINUS, ast.ops.UPLUS)):

        if unop.op == ast.ops.UMINUS:
            op_val = -evaluate_to_python_val(unop.expr, schema=schema)

            if result_type.issubclass(int_t):
                return irast.IntegerConstant(
                    value=str(op_val), type=result_type)
            else:
                return irast.FloatConstant(
                    value=str(op_val), type=result_type)
        else:
            return evaluate(unop.expr, schema=schema)

    raise UnsupportedExpressionError(
        f'unexpected unary operation: {unop.op} {result_type.displayname}',
        context=unop.context)


def evaluate_comparison_binop(
        binop: irast.BinOp,
        schema: s_schema.Schema) -> irast.BooleanConstant:

    op = binop.op
    left = evaluate(binop.left, schema=schema)
    left_val = const_to_python(left, schema=schema)
    right = evaluate(binop.right, schema=schema)
    right_val = const_to_python(right, schema=schema)

    if op == ast.ops.EQ:
        value = left_val == right_val
    elif op == ast.ops.NE:
        value = left_val != right_val
    elif op == ast.ops.GT:
        value = left_val > right_val
    elif op == ast.ops.GE:
        value = left_val >= right_val
    elif op == ast.ops.LT:
        value = left_val < right_val
    elif op == ast.ops.LE:
        value = left_val <= right_val
    else:
        raise UnsupportedExpressionError(
            f'unexpected operator: {op}',
            context=binop.context)

    return irast.BooleanConstant(
        value='true' if value else 'false',
        type=schema.get('std::bool')
    )


def evaluate_arithmetic_binop(
        binop: irast.BinOp,
        schema: s_schema.Schema) -> irast.BaseConstant:

    op = binop.op
    left = evaluate(binop.left, schema=schema)
    left_val = const_to_python(left, schema=schema)
    right = evaluate(binop.right, schema=schema)
    right_val = const_to_python(right, schema=schema)

    real_t = schema.get('std::anyreal')
    int_t = schema.get('std::anyint')

    left_type = irutils.infer_type(left, schema)
    right_type = irutils.infer_type(right, schema)

    if not left_type.issubclass(real_t) or not right_type.issubclass(real_t):
        return

    result_type = s_scalars.get_op_type(
        op, left_type, right_type, schema=schema)

    if op == ast.ops.ADD:
        value = left_val + right_val
    elif op == ast.ops.SUB:
        value = left_val - right_val
    elif op == ast.ops.MUL:
        value = left_val * right_val
    elif op == ast.ops.DIV:
        if left_type.issubclass(int_t) and right_type.issubclass(int_t):
            value = left_val // right_val
        else:
            value = left_val / right_val
    elif op == ast.ops.POW:
        value = left_val ** right_val
    elif op == ast.ops.MOD:
        value = left_val % right_val
    else:
        raise UnsupportedExpressionError(
            f'unexpected operator: {op}',
            context=binop.context)

    if result_type.issubclass(int_t):
        return irast.IntegerConstant(value=str(value), type=result_type)
    else:
        return irast.FloatConstant(value=str(value), type=result_type)


@functools.singledispatch
def const_to_python(
        ir: irast.BaseConstant,
        schema: s_schema.Schema) -> object:
    raise NotImplementedError(
        f'cannot convert {ir.__class__} to a Python value')


@const_to_python.register(irast.IntegerConstant)
def int_const_to_python(
        ir: irast.IntegerConstant,
        schema: s_schema.Schema) -> object:

    if ir.type.name == 'std::decimal':
        return decimal.Decimal(ir.value)
    else:
        return int(ir.value)


@const_to_python.register(irast.FloatConstant)
def float_const_to_python(
        ir: irast.FloatConstant,
        schema: s_schema.Schema) -> object:

    if ir.type.name == 'std::decimal':
        return decimal.Decimal(ir.value)
    else:
        return float(ir.value)


@const_to_python.register(irast.StringConstant)
def str_const_to_python(
        ir: irast.StringConstant,
        schema: s_schema.Schema) -> str:

    return ql_lexutils.unescape_string(ir.value)


@const_to_python.register(irast.RawStringConstant)
def raw_str_const_to_python(
        ir: irast.RawStringConstant,
        schema: s_schema.Schema) -> str:

    return ir.value


@const_to_python.register(irast.BooleanConstant)
def bool_const_to_python(
        ir: irast.BooleanConstant,
        schema: s_schema.Schema) -> bool:

    return ir.value == 'true'
