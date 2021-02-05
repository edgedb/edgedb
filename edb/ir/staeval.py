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


from __future__ import annotations
from typing import *

import dataclasses
import decimal
import functools

from edb import errors

from edb.common import typeutils
from edb.common import uuidgen
from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils
from edb.ir import utils as irutils

from edb.schema import objects as s_obj
from edb.schema import objtypes as s_objtypes
from edb.schema import types as s_types
from edb.schema import scalars as s_scalars
from edb.schema import schema as s_schema
from edb.schema import constraints as s_constr

from edb.server import config


class StaticEvaluationError(errors.QueryError):
    pass


class UnsupportedExpressionError(errors.QueryError):
    pass


def evaluate_to_python_val(
    ir: irast.Base,
    schema: s_schema.Schema,
) -> Any:
    const: Union[irast.TypeCast, irast.ConstExpr]
    if isinstance(ir, irast.Set) and isinstance(ir.expr, irast.TypeCast):
        # Special case for type casts.
        # We cannot fold them, but can eval to Python
        const = ir.expr
    else:
        const = evaluate(ir, schema=schema)
    return const_to_python(const, schema=schema)


@functools.singledispatch
def evaluate(
        ir: irast.Base,
        schema: s_schema.Schema) -> irast.ConstExpr:
    raise UnsupportedExpressionError(
        f'no static IR evaluation handler for {ir.__class__}')


@evaluate.register(irast.SelectStmt)
def evaluate_SelectStmt(
        ir_stmt: irast.SelectStmt,
        schema: s_schema.Schema) -> irast.ConstExpr:

    if irutils.is_trivial_select(ir_stmt):
        return evaluate(ir_stmt.result, schema)
    else:
        raise UnsupportedExpressionError(
            'expression is not constant', context=ir_stmt.context)


@evaluate.register(irast.EmptySet)
def evaluate_EmptySet(
        ir_set: irast.EmptySet,
        schema: s_schema.Schema) -> irast.ConstExpr:
    return ir_set


@evaluate.register(irast.Set)
def evaluate_Set(
        ir_set: irast.Set,
        schema: s_schema.Schema) -> irast.ConstExpr:
    if ir_set.expr is not None:
        return evaluate(ir_set.expr, schema=schema)
    else:
        raise UnsupportedExpressionError(
            'expression is not constant', context=ir_set.context)


@evaluate.register(irast.ConstExpr)
def evaluate_BaseConstant(
        ir_const: irast.ConstExpr,
        schema: s_schema.Schema) -> irast.ConstExpr:
    return ir_const


op_table = {
    # Concatenation
    ('Infix', 'std::++'): lambda a, b: a + b,
}


@evaluate.register(irast.OperatorCall)
def evaluate_OperatorCall(
        opcall: irast.OperatorCall,
        schema: s_schema.Schema) -> irast.ConstExpr:

    if irutils.is_union_expr(opcall):
        return _evaluate_union(opcall, schema)

    eval_func = op_table.get(
        (opcall.operator_kind, str(opcall.func_shortname)),
    )
    if eval_func is None:
        raise UnsupportedExpressionError(
            f'unsupported operator: {opcall.func_shortname}',
            context=opcall.context)

    args = []
    for arg in opcall.args:
        arg_val = evaluate_to_python_val(arg.expr, schema=schema)
        if isinstance(arg_val, tuple):
            raise UnsupportedExpressionError(
                f'non-singleton operations are not supported',
                context=opcall.context)
        if arg_val is None:
            raise UnsupportedExpressionError(
                f'empty operations are not supported',
                context=opcall.context)

        args.append(arg_val)

    value = eval_func(*args)
    # Since we only perform string concatenations here, the constant
    # in question is always a StringConstant.
    qlconst = qlast.StringConstant.from_python(value)

    result = qlcompiler.compile_constant_tree_to_ir(
        qlconst, styperef=opcall.typeref, schema=schema)

    assert isinstance(result, irast.ConstExpr), 'expected ConstExpr'
    return result


def _evaluate_union(
        opcall: irast.OperatorCall,
        schema: s_schema.Schema) -> irast.ConstantSet:

    elements: List[irast.ConstExpr] = []
    for arg in opcall.args:
        val = evaluate(arg.expr, schema=schema)
        if isinstance(val, irast.ConstantSet):
            elements.extend(val.elements)
        else:
            elements.append(val)

    return irast.ConstantSet(
        elements=tuple(elements),
        typeref=next(iter(elements)).typeref,
    )


@functools.singledispatch
def const_to_python(
        ir: irast.ConstExpr,
        schema: s_schema.Schema) -> Any:
    raise UnsupportedExpressionError(
        f'cannot convert {ir!r} to Python value')


@const_to_python.register(irast.EmptySet)
def empty_set_to_python(
    ir: irast.EmptySet,
    schema: s_schema.Schema,
) -> None:
    return None


@const_to_python.register(irast.ConstantSet)
def const_set_to_python(
        ir: irast.ConstantSet,
        schema: s_schema.Schema) -> Tuple[Any, ...]:
    return tuple(const_to_python(v, schema) for v in ir.elements)


@const_to_python.register(irast.IntegerConstant)
def int_const_to_python(
        ir: irast.IntegerConstant,
        schema: s_schema.Schema) -> Any:

    stype = schema.get_by_id(ir.typeref.id)
    assert isinstance(stype, s_types.Type)
    bigint = schema.get('std::bigint', type=s_obj.SubclassableObject)
    if stype.issubclass(schema, bigint):
        return decimal.Decimal(ir.value)
    else:
        return int(ir.value)


@const_to_python.register(irast.FloatConstant)
def float_const_to_python(
        ir: irast.FloatConstant,
        schema: s_schema.Schema) -> Any:

    stype = schema.get_by_id(ir.typeref.id)
    assert isinstance(stype, s_types.Type)
    bigint = schema.get('std::bigint', type=s_obj.SubclassableObject)
    if stype.issubclass(schema, bigint):
        return decimal.Decimal(ir.value)
    else:
        return float(ir.value)


@const_to_python.register(irast.StringConstant)
def str_const_to_python(
        ir: irast.StringConstant,
        schema: s_schema.Schema) -> Any:

    return ir.value


@const_to_python.register(irast.BooleanConstant)
def bool_const_to_python(
        ir: irast.BooleanConstant,
        schema: s_schema.Schema) -> Any:

    return ir.value == 'true'


@const_to_python.register(irast.TypeCast)
def cast_const_to_python(
        ir: irast.TypeCast,
        schema: s_schema.Schema) -> Any:

    schema, stype = irtyputils.ir_typeref_to_type(schema, ir.to_type)
    pytype = scalar_type_to_python_type(stype, schema)
    sval = evaluate_to_python_val(ir.expr, schema=schema)
    return pytype(sval)


def schema_type_to_python_type(
        stype: s_types.Type,
        schema: s_schema.Schema) -> type:
    if isinstance(stype, s_scalars.ScalarType):
        return scalar_type_to_python_type(stype, schema)
    elif isinstance(stype, s_objtypes.ObjectType):
        return object_type_to_python_type(stype, schema)
    else:
        raise UnsupportedExpressionError(
            f'{stype.get_displayname(schema)} is not representable in Python')


def scalar_type_to_python_type(
        stype: s_types.Type,
        schema: s_schema.Schema) -> type:

    typemap = {
        'std::str': str,
        'std::anyint': int,
        'std::anyfloat': float,
        'std::decimal': decimal.Decimal,
        'std::bigint': decimal.Decimal,
        'std::bool': bool,
        'std::json': str,
        'std::uuid': uuidgen.UUID,
    }

    for basetype_name, python_type in typemap.items():
        basetype = schema.get(basetype_name, type=s_obj.InheritingObject)
        if stype.issubclass(schema, basetype):
            return python_type

    raise UnsupportedExpressionError(
        f'{stype.get_displayname(schema)} is not representable in Python')


def object_type_to_python_type(
        objtype: s_objtypes.ObjectType,
        schema: s_schema.Schema, *,
        base_class: Optional[type] = None,
        _memo: Optional[Dict[s_types.Type, type]]=None
) -> type:

    if _memo is None:
        _memo = {}
    default: Any
    fields = []
    subclasses = []

    for pn, p in objtype.get_pointers(schema).items(schema):
        str_pn = str(pn)
        if str_pn in ('id', '__type__'):
            continue

        ptype = p.get_target(schema)
        assert ptype is not None

        if isinstance(ptype, s_objtypes.ObjectType):
            pytype = _memo.get(ptype)
            if pytype is None:
                pytype = object_type_to_python_type(
                    ptype, schema, base_class=base_class, _memo=_memo)
                _memo[ptype] = pytype

                for subtype in ptype.children(schema):
                    subclasses.append(
                        object_type_to_python_type(
                            subtype, schema,
                            base_class=pytype, _memo=_memo))
        else:
            pytype = scalar_type_to_python_type(ptype, schema)

        ptr_card = p.get_cardinality(schema)
        is_multi = ptr_card.is_multi()
        if is_multi:
            pytype = FrozenSet[pytype]  # type: ignore

        default = p.get_default(schema)
        if default is None:
            if p.get_required(schema):
                default = dataclasses.MISSING
        else:
            default = qlcompiler.evaluate_to_python_val(
                default.text, schema=schema)
            if is_multi and not isinstance(default, frozenset):
                default = frozenset((default,))

        constraints = p.get_constraints(schema).objects(schema)
        exclusive = schema.get('std::exclusive', type=s_constr.Constraint)
        unique = (
            not ptype.is_object_type()
            and any(c.issubclass(schema, exclusive) for c in constraints)
        )
        field = dataclasses.field(
            compare=unique,
            hash=unique,
            repr=True,
            default=default,
        )
        fields.append((str_pn, pytype, field))

    bases: Tuple[type, ...]
    if base_class is not None:
        bases = (base_class,)
    else:
        bases = ()

    ptype_dataclass = dataclasses.make_dataclass(
        objtype.get_name(schema).name,
        fields=fields,
        bases=bases,
        frozen=True,
        namespace={'_subclasses': subclasses},
    )
    assert isinstance(ptype_dataclass, type)
    return ptype_dataclass


@functools.singledispatch
def evaluate_to_config_op(
        ir: irast.Base,
        schema: s_schema.Schema) -> Any:
    raise UnsupportedExpressionError(
        f'no config op evaluation handler for {ir.__class__}')


@evaluate_to_config_op.register
def evaluate_config_set(
        ir: irast.ConfigSet,
        schema: s_schema.Schema) -> Any:

    value = evaluate_to_python_val(ir.expr, schema)
    if ir.cardinality is qltypes.SchemaCardinality.Many:
        if value is None:
            value = []
        elif not typeutils.is_container(value):
            value = [value]

    return config.Operation(
        opcode=config.OpCode.CONFIG_SET,
        scope=ir.scope,
        setting_name=ir.name,
        value=value,
    )


@evaluate_to_config_op.register
def evaluate_config_reset(
        ir: irast.ConfigReset,
        schema: s_schema.Schema) -> Any:

    if ir.selector is not None:
        raise UnsupportedExpressionError(
            'filtered CONFIGURE RESET is not supported by static eval'
        )

    return config.Operation(
        opcode=config.OpCode.CONFIG_RESET,
        scope=ir.scope,
        setting_name=ir.name,
        value=None,
    )
