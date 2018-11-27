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

from edb import errors

from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import compiler as ql_compiler
from edb.lang.edgeql.parser.grammar import lexutils as ql_lexutils

from edb.lang.ir import ast as irast

from edb.lang.schema import schema as s_schema


class StaticEvaluationError(errors.QueryError):
    pass


class UnsupportedExpressionError(errors.QueryError):
    pass


def evaluate_to_python_val(
        ir: irast.Base,
        schema: s_schema.Schema) -> object:
    const = evaluate(ir, schema=schema)
    return const_to_python(const, schema=schema)


@functools.singledispatch
def evaluate(
        ir: irast.Base,
        schema: s_schema.Schema) -> irast.BaseConstant:
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


op_table = {
    # Arithmetic
    ('PREFIX', 'std::+'): lambda a: a,
    ('PREFIX', 'std::-'): lambda a: -a,
    ('INFIX', 'std::+'): lambda a, b: a + b,
    ('INFIX', 'std::-'): lambda a, b: a - b,
    ('INFIX', 'std::*'): lambda a, b: a * b,
    ('INFIX', 'std::/'): lambda a, b: a / b,
    ('INFIX', 'std:://'): lambda a, b: a // b,
    ('INFIX', 'std::%'): lambda a, b: a % b,
    ('INFIX', 'std::^'): lambda a, b: (
        decimal.Decimal(a) ** decimal.Decimal(b)
        if isinstance(a, int) else a ** b),

    # Comparison
    ('INFIX', 'std::='): lambda a, b: a == b,
    ('INFIX', 'std::!='): lambda a, b: a != b,
    ('INFIX', 'std::>'): lambda a, b: a > b,
    ('INFIX', 'std::>='): lambda a, b: a >= b,
    ('INFIX', 'std::<'): lambda a, b: a < b,
    ('INFIX', 'std::<='): lambda a, b: a <= b,

    # Concatenation
    ('INFIX', 'std::++'): lambda a, b: a + b,
}


@evaluate.register(irast.OperatorCall)
def evaluate_OperatorCall(
        opcall: irast.OperatorCall,
        schema: s_schema.Schema) -> irast.BaseConstant:

    eval_func = op_table.get((opcall.operator_kind, opcall.func_shortname))
    if eval_func is None:
        raise UnsupportedExpressionError(
            f'unsupported operator: {opcall.func_shortname}',
            context=opcall.context)

    args = []
    for arg in opcall.args:
        args.append(evaluate_to_python_val(arg, schema=schema))

    value = eval_func(*args)
    qlconst = qlast.BaseConstant.from_python(value)

    result = ql_compiler.compile_constant_tree_to_ir(
        qlconst, stype=opcall.stype, schema=schema)

    return result


@functools.singledispatch
def const_to_python(
        ir: irast.BaseConstant,
        schema: s_schema.Schema) -> object:
    raise UnsupportedExpressionError(
        f'cannot convert {ir!r} to Python value')


@const_to_python.register(irast.IntegerConstant)
def int_const_to_python(
        ir: irast.IntegerConstant,
        schema: s_schema.Schema) -> object:

    if ir.stype.get_name(schema) == 'std::decimal':
        return decimal.Decimal(ir.value)
    else:
        return int(ir.value)


@const_to_python.register(irast.FloatConstant)
def float_const_to_python(
        ir: irast.FloatConstant,
        schema: s_schema.Schema) -> object:

    if ir.stype.get_name(schema) == 'std::decimal':
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
