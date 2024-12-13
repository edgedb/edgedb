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
from typing import (
    Any,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Dict,
    List,
    FrozenSet,
)

import decimal
import functools
import uuid

import immutables


from edb import errors

from edb.common import typeutils
from edb.common import parsing
from edb.common import uuidgen
from edb.common import value_dispatch
from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils
from edb.ir import statypes as statypes
from edb.ir import utils as irutils

from edb.schema import name as sn
from edb.schema import objects as s_obj
from edb.schema import objtypes as s_objtypes
from edb.schema import types as s_types
from edb.schema import scalars as s_scalars
from edb.schema import schema as s_schema
from edb.schema import constraints as s_constr
from edb.schema import pointers as s_pointers

from edb.server import config


class StaticEvaluationError(errors.QueryError):
    pass


class UnsupportedExpressionError(errors.QueryError):
    pass


EvaluationResult = irast.TypeCast | irast.ConstExpr | irast.Array | irast.Tuple


def evaluate_to_python_val(
    ir: irast.Base,
    schema: s_schema.Schema,
) -> Any:
    const = evaluate(ir, schema=schema)
    return const_to_python(const, schema=schema)


@functools.singledispatch
def evaluate(ir: irast.Base, schema: s_schema.Schema) -> EvaluationResult:
    raise UnsupportedExpressionError(
        f'no static IR evaluation handler for {ir.__class__}')


@evaluate.register(irast.SelectStmt)
def evaluate_SelectStmt(
    ir_stmt: irast.SelectStmt, schema: s_schema.Schema
) -> EvaluationResult:

    if irutils.is_trivial_select(ir_stmt) and not ir_stmt.result.is_binding:
        return evaluate(ir_stmt.result, schema)
    else:
        raise UnsupportedExpressionError(
            'expression is not constant', span=ir_stmt.span)


@evaluate.register(irast.InsertStmt)
def evaluate_InsertStmt(
    ir: irast.InsertStmt, schema: s_schema.Schema
) -> EvaluationResult:
    # InsertStmt should NOT be statically evaluated in general;
    # This is a special case for inserting nested cfg::ConfigObject
    # when it's evaluated into a named tuple and then squashed into
    # a Python dict to be used in compile_structured_config().
    tmp_schema, subject_type = irtyputils.ir_typeref_to_type(
        schema, ir.subject.expr.typeref
    )
    config_obj = schema.get("cfg::ConfigObject")
    assert isinstance(config_obj, s_obj.SubclassableObject)
    if subject_type.issubclass(tmp_schema, config_obj):
        return irast.Tuple(
            named=True,
            typeref=ir.subject.typeref,
            elements=[
                irast.TupleElement(
                    name=ptr_set.expr.ptrref.shortname.name,
                    val=irast.Set(
                        expr=evaluate(ptr_set.expr.expr, schema),
                        typeref=ptr_set.typeref,
                        path_id=ptr_set.path_id,
                    ),
                )
                for ptr_set, _ in ir.subject.shape
                if ptr_set.expr.ptrref.shortname.name != "id"
                and ptr_set.expr.expr is not None
            ],
        )

    raise UnsupportedExpressionError(
        f'no static IR evaluation handler for general {ir.__class__}'
    )


@evaluate.register(irast.TypeIntrospection)
def evaluate_TypeIntrospection(
    ir: irast.TypeIntrospection, schema: s_schema.Schema
) -> EvaluationResult:
    return irast.StaticIntrospection(
        named=True, ir=ir, schema=schema, elements=[], typeref=ir.typeref
    )


@evaluate.register(irast.TypeCast)
def evaluate_TypeCast(
    ir_cast: irast.TypeCast, schema: s_schema.Schema
) -> EvaluationResult:

    schema, from_type = irtyputils.ir_typeref_to_type(
        schema, ir_cast.from_type)
    schema, to_type = irtyputils.ir_typeref_to_type(
        schema, ir_cast.to_type)

    if (
        not isinstance(from_type, s_scalars.ScalarType)
        or not isinstance(to_type, s_scalars.ScalarType)
    ):
        raise UnsupportedExpressionError('object cast not supported')
    scalar_type_to_python_type(from_type, schema)
    scalar_type_to_python_type(to_type, schema)
    evaluate(ir_cast.expr, schema)
    return ir_cast


@evaluate.register(irast.EmptySet)
def evaluate_EmptySet(
    ir_set: irast.EmptySet, schema: s_schema.Schema
) -> EvaluationResult:
    return ir_set


@evaluate.register(irast.Set)
def evaluate_Set(
        ir_set: irast.Set,
        schema: s_schema.Schema) -> EvaluationResult:
    return evaluate(ir_set.expr, schema=schema)


@evaluate.register
def evaluate_Pointer(
    ptr: irast.Pointer, schema: s_schema.Schema
) -> EvaluationResult:
    if ptr.expr is not None:
        return evaluate(ptr.expr, schema=schema)

    elif (
        ptr.direction == s_pointers.PointerDirection.Outbound
        and isinstance(ptr.ptrref, irast.PointerRef)
        and ptr.ptrref.out_cardinality.is_single()
        and ptr.ptrref.out_target.is_scalar
    ):
        return evaluate_pointer_ref(
            evaluate(ptr.source.expr, schema=schema), ptr.ptrref
        )

    else:
        raise UnsupportedExpressionError(
            'expression is not constant', span=ptr.span)


@functools.singledispatch
def evaluate_pointer_ref(
    evaluated_source: EvaluationResult, ptrref: irast.PointerRef
) -> EvaluationResult:
    raise UnsupportedExpressionError(
        f'unsupported PointerRef on source {evaluated_source}',
        span=ptrref.span,
    )


@evaluate_pointer_ref.register(irast.StaticIntrospection)
def evaluate_pointer_ref_StaticIntrospection(
    source: irast.StaticIntrospection, ptrref: irast.PointerRef
) -> EvaluationResult:
    return source.get_field_value(ptrref.shortname)


@evaluate.register(irast.ConstExpr)
def evaluate_BaseConstant(
    ir_const: irast.ConstExpr, schema: s_schema.Schema
) -> EvaluationResult:
    return ir_const


@evaluate.register(irast.Array)
def evaluate_Array(
    ir: irast.Array, schema: s_schema.Schema
) -> EvaluationResult:
    return irast.Array(
        elements=tuple(
            x.replace(expr=evaluate(x, schema)) for x in ir.elements
        ),
        typeref=ir.typeref,
    )


@evaluate.register(irast.Tuple)
def evaluate_Tuple(
    ir: irast.Tuple, schema: s_schema.Schema
) -> EvaluationResult:
    return irast.Tuple(
        named=ir.named,
        elements=[
            x.replace(
                val=x.val.replace(
                    expr=evaluate(x.val, schema)
                ),
            ) for x in ir.elements
        ],
        typeref=ir.typeref,
    )


def _process_op_result(
    value: object,
    typeref: irast.TypeRef,
    schema: s_schema.Schema,
    *,
    span: Optional[parsing.Span]=None,
) -> irast.ConstExpr:
    qlconst: qlast.BaseConstant
    if isinstance(value, str):
        qlconst = qlast.Constant.string(value)
    elif isinstance(value, bool):
        qlconst = qlast.Constant.boolean(value)
    else:
        raise UnsupportedExpressionError(
            f"unsupported result type: {type(value)}", span=span
        )

    result = qlcompiler.compile_constant_tree_to_ir(
        qlconst, styperef=typeref, schema=schema)

    assert isinstance(result, irast.ConstExpr), 'expected ConstExpr'
    return result


op_table = {
    # Concatenation
    ('Infix', 'std::++'): lambda a, b: a + b,
    ('Infix', 'std::>='): lambda a, b: a >= b,
    ('Infix', 'std::>'): lambda a, b: a > b,
    ('Infix', 'std::<='): lambda a, b: a <= b,
    ('Infix', 'std::<'): lambda a, b: a < b,
    ('Infix', 'std::='): lambda a, b: a == b,
    ('Infix', 'std::!='): lambda a, b: a != b,
}


@evaluate.register(irast.OperatorCall)
def evaluate_OperatorCall(
    opcall: irast.OperatorCall, schema: s_schema.Schema
) -> irast.ConstExpr:

    if irutils.is_union_expr(opcall):
        return _evaluate_union(opcall, schema)

    eval_func = op_table.get(
        (opcall.operator_kind, str(opcall.func_shortname)),
    )
    if eval_func is None:
        raise UnsupportedExpressionError(
            f'unsupported operator: {opcall.func_shortname}',
            span=opcall.span)

    args: Dict[int, irast.CallArg] = {}
    for key, arg in opcall.args.items():
        arg_val = evaluate_to_python_val(arg.expr, schema=schema)
        if isinstance(arg_val, tuple):
            raise UnsupportedExpressionError(
                f'non-singleton operations are not supported',
                span=opcall.span)
        if arg_val is None:
            raise UnsupportedExpressionError(
                f'empty operations are not supported',
                span=opcall.span)
        if isinstance(key, str):
            raise UnsupportedExpressionError(
                f'named arguments are not allowed for operators',
                span=opcall.span)

        args[key] = arg_val

    args_list: List[irast.CallArg] = []
    for key in range(len(args)):
        if key not in args:
            raise UnsupportedExpressionError(
                f'missing positional argument {key}',
                span=opcall.span)

        args_list.append(args[key])

    value = eval_func(*args_list)
    return _process_op_result(
        value, opcall.typeref, schema, span=opcall.span)


@evaluate.register(irast.SliceIndirection)
def evaluate_SliceIndirection(
    slice: irast.SliceIndirection, schema: s_schema.Schema
) -> irast.ConstExpr:

    args = [slice.expr, slice.start, slice.stop]
    vals = [
        evaluate_to_python_val(arg, schema=schema) if arg else None
        for arg in args
    ]

    for arg, arg_val in zip(args, vals):
        if arg is None:
            continue
        if isinstance(arg_val, tuple):
            raise UnsupportedExpressionError(
                f'non-singleton operations are not supported',
                span=slice.span)
        if arg_val is None:
            raise UnsupportedExpressionError(
                f'empty operations are not supported',
                span=slice.span)

    base, start, stop = vals

    value = base[start:stop]  # type: ignore[index]
    return _process_op_result(
        value, slice.expr.typeref, schema, span=slice.span)


def _evaluate_union(
    opcall: irast.OperatorCall, schema: s_schema.Schema
) -> irast.ConstExpr:

    elements: List[irast.BaseConstant] = []
    for arg in opcall.args.values():
        val = evaluate(arg.expr, schema=schema)
        if isinstance(val, irast.TypeCast):
            val = evaluate(val.expr, schema=schema)
        if isinstance(val, irast.ConstantSet):
            for el in val.elements:
                if isinstance(el, irast.Parameter):
                    raise UnsupportedExpressionError(
                        f'{el!r} not supported in UNION',
                        span=opcall.span)
                elements.append(el)
        elif isinstance(val, irast.EmptySet):
            empty_set = val
        elif isinstance(val, irast.BaseConstant):
            elements.append(val)
        else:
            raise UnsupportedExpressionError(
                f'{val!r} not supported in UNION',
                span=opcall.span)

    if elements:
        return irast.ConstantSet(
            elements=tuple(elements),
            typeref=next(iter(elements)).typeref,
        )
    else:
        # We get an empty set if the UNION was exclusivly empty set
        # literals. If that happens, grab one of the empty sets
        # that we saw and return it.
        return empty_set


@functools.singledispatch
def const_to_python(ir: irast.Expr | None, schema: s_schema.Schema) -> Any:
    raise UnsupportedExpressionError(f'cannot convert {ir!r} to Python value')


@const_to_python.register(irast.EmptySet)
def empty_set_to_python(
    ir: irast.EmptySet,
    schema: s_schema.Schema,
) -> None:
    return None


@const_to_python.register(irast.ConstantSet)
def const_set_to_python(
    ir: irast.ConstantSet, schema: s_schema.Schema
) -> Tuple[Any, ...]:
    return tuple(const_to_python(v, schema) for v in ir.elements)


@const_to_python.register(irast.Array)
def array_const_to_python(ir: irast.Array, schema: s_schema.Schema) -> Any:
    return [const_to_python(x.expr, schema) for x in ir.elements]


@const_to_python.register(irast.Tuple)
def tuple_const_to_python(ir: irast.Tuple, schema: s_schema.Schema) -> Any:
    if ir.named:
        return {
            x.name: const_to_python(x.val.expr, schema) for x in ir.elements
        }
    else:
        return tuple(
            const_to_python(x.val.expr, schema) for x in ir.elements
        )


@const_to_python.register(irast.IntegerConstant)
def int_const_to_python(
    ir: irast.IntegerConstant, schema: s_schema.Schema
) -> Any:

    stype = schema.get_by_id(ir.typeref.id)
    assert isinstance(stype, s_types.Type)
    bigint = schema.get('std::bigint', type=s_obj.SubclassableObject)
    if stype.issubclass(schema, bigint):
        return decimal.Decimal(ir.value)
    else:
        return int(ir.value)


@const_to_python.register(irast.FloatConstant)
def float_const_to_python(
    ir: irast.FloatConstant, schema: s_schema.Schema
) -> Any:

    stype = schema.get_by_id(ir.typeref.id)
    assert isinstance(stype, s_types.Type)
    bigint = schema.get('std::bigint', type=s_obj.SubclassableObject)
    if stype.issubclass(schema, bigint):
        return decimal.Decimal(ir.value)
    else:
        return float(ir.value)


@const_to_python.register(irast.StringConstant)
def str_const_to_python(
    ir: irast.StringConstant, schema: s_schema.Schema
) -> Any:

    return ir.value


@const_to_python.register(irast.BooleanConstant)
def bool_const_to_python(
    ir: irast.BooleanConstant, schema: s_schema.Schema
) -> Any:

    return ir.value == 'true'


@const_to_python.register(irast.TypeCast)
def cast_const_to_python(ir: irast.TypeCast, schema: s_schema.Schema) -> Any:

    schema, stype = irtyputils.ir_typeref_to_type(schema, ir.to_type)
    pytype = scalar_type_to_python_type(stype, schema)
    sval = evaluate_to_python_val(ir.expr, schema=schema)
    return python_cast(sval, pytype)


@functools.singledispatch
def python_cast(sval: Any, pytype: type) -> Any:
    return pytype(sval)


@python_cast.register(type(None))
def python_cast_none(sval: None, pytype: type) -> None:
    return None


@python_cast.register(tuple)
def python_cast_tuple(sval: Tuple[Any, ...], pytype: type) -> Any:
    return tuple(python_cast(elem, pytype) for elem in sval)


@python_cast.register(str)
def python_cast_str(sval: str, pytype: type) -> Any:
    if pytype is bool:
        if sval.lower() == 'true':
            return True
        elif sval.lower() == 'false':
            return False
        else:
            raise errors.InvalidValueError(
                f"invalid input syntax for type bool: {sval!r}",
                hint="bool value can only be one of: true, false"
            )
    else:
        return pytype(sval)


def schema_type_to_python_type(
    stype: s_types.Type, schema: s_schema.Schema
) -> type | statypes.CompositeTypeSpec:
    if isinstance(stype, s_scalars.ScalarType):
        return scalar_type_to_python_type(stype, schema)
    elif isinstance(stype, s_objtypes.ObjectType):
        return object_type_to_spec(
            stype, schema, spec_class=statypes.CompositeTypeSpec)
    else:
        raise UnsupportedExpressionError(
            f'{stype.get_displayname(schema)} is not representable in Python')


typemap = {
    'std::str': str,
    'std::anyint': int,
    'std::anyfloat': float,
    'std::decimal': decimal.Decimal,
    'std::bigint': decimal.Decimal,
    'std::bool': bool,
    'std::json': str,
    'std::uuid': uuidgen.UUID,
    'std::duration': statypes.Duration,
    'cfg::memory': statypes.ConfigMemory,
}


def scalar_type_to_python_type(
    stype: s_types.Type,
    schema: s_schema.Schema,
) -> type:
    for basetype_name, pytype in typemap.items():
        basetype = schema.get(
            basetype_name, type=s_scalars.ScalarType, default=None)
        if basetype and stype.issubclass(schema, basetype):
            return pytype

    if stype.is_enum(schema):
        return str

    raise UnsupportedExpressionError(
        f'{stype.get_displayname(schema)} is not representable in Python')


T_spec = TypeVar('T_spec', bound=statypes.CompositeTypeSpec)


class _Missing:
    pass


def object_type_to_spec(
    objtype: s_objtypes.ObjectType,
    schema: s_schema.Schema,
    *,
    # We pass a spec_class so that users like the config system can ask for
    # their own subtyped versions of a spec.
    spec_class: Type[T_spec],
    parent: Optional[T_spec] = None,
    _memo: Optional[Dict[s_types.Type, T_spec | type]] = None,
) -> T_spec:
    if _memo is None:
        _memo = {}
    # Prevent infinite recursion
    _memo[objtype] = _Missing

    default: Any
    fields = {}

    for pn, p in objtype.get_pointers(schema).items(schema):
        assert isinstance(p, s_pointers.Pointer)
        str_pn = str(pn)
        if str_pn in ('id', '__type__'):
            continue

        ptype = p.get_target(schema)
        assert ptype is not None

        if isinstance(ptype, s_objtypes.ObjectType):
            pytype = _memo.get(ptype)
            if pytype is _Missing:
                raise UnsupportedExpressionError()
            if pytype is None:
                pytype = object_type_to_spec(
                    ptype, schema, spec_class=spec_class,
                    parent=parent, _memo=_memo)
                _memo[ptype] = pytype
        else:
            pytype = scalar_type_to_python_type(ptype, schema)

        ptr_card: qltypes.SchemaCardinality = p.get_cardinality(schema)
        if ptr_card.is_known():
            is_multi = ptr_card.is_multi()
        else:
            raise UnsupportedExpressionError()

        if is_multi:
            pytype = FrozenSet[pytype]  # type: ignore

        default = p.get_default(schema)
        if default is None:
            if p.get_required(schema):
                default = statypes.MISSING
        else:
            default = qlcompiler.evaluate_to_python_val(
                default.text, schema=schema)
            if is_multi and not isinstance(default, frozenset):
                default = frozenset((default,))

        constraints = p.get_constraints(schema).objects(schema)
        exclusive = schema.get('std::exclusive', type=s_constr.Constraint)
        unique = (
            not ptype.is_object_type()
            and any(
                c.issubclass(schema, exclusive) and not c.get_delegated(schema)
                for c in constraints
            )
        )
        fields[str_pn] = statypes.CompositeTypeSpecField(
            name=str_pn,
            type=pytype,
            unique=unique,
            default=default,
            secret=p.get_secret(schema),
            protected=p.get_protected(schema),
        )

    spec = spec_class(
        name=str(objtype.get_name(schema)),
        fields=immutables.Map(fields),
        parent=parent,
    )

    for subtype in objtype.children(schema):
        spec.children.append(
            object_type_to_spec(
                subtype, schema,
                spec_class=spec_class,
                parent=spec, _memo=_memo))

    return spec


@functools.singledispatch
def evaluate_to_config_op(
    ir: irast.Base, schema: s_schema.Schema
) -> config.Operation:
    raise UnsupportedExpressionError(
        f'no config op evaluation handler for {ir.__class__}')


@evaluate_to_config_op.register(irast.ConfigSet)
def evaluate_config_set(
    ir: irast.ConfigSet, schema: s_schema.Schema
) -> config.Operation:

    if ir.scope == qltypes.ConfigScope.GLOBAL:
        raise UnsupportedExpressionError(
            'SET GLOBAL is not supported by static eval'
        )

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


@evaluate_to_config_op.register(irast.ConfigReset)
def evaluate_config_reset(
    ir: irast.ConfigReset, schema: s_schema.Schema
) -> config.Operation:

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


@evaluate_to_config_op.register(irast.ConfigInsert)
def evaluate_config_insert(
    ir: irast.ConfigInsert, schema: s_schema.Schema
) -> config.Operation:
    return config.Operation(
        opcode=config.OpCode.CONFIG_ADD,
        scope=ir.scope,
        setting_name=ir.name,
        value=evaluate_to_python_val(
            irast.InsertStmt(subject=ir.expr), schema=schema
        ),
    )


@value_dispatch.value_dispatch
def coerce_py_const(
    type_id: uuid.UUID, val: Any
) -> irast.ConstExpr | irast.TypeCast:
    raise UnsupportedExpressionError(f"unimplemented coerce type: {type_id}")


@coerce_py_const.register(s_obj.get_known_type_id("std::str"))
def evaluate_std_str(
    type_id: uuid.UUID, val: Any
) -> irast.ConstExpr | irast.TypeCast:
    return irast.StringConstant(
        typeref=irast.TypeRef(
            id=type_id, name_hint=sn.name_from_string("std::str")
        ),
        value=str(val),
    )


@coerce_py_const.register(s_obj.get_known_type_id("std::bool"))
def evaluate_std_bool(
    type_id: uuid.UUID, val: Any
) -> irast.ConstExpr | irast.TypeCast:
    return irast.BooleanConstant(
        typeref=irast.TypeRef(
            id=type_id, name_hint=sn.name_from_string("std::bool")
        ),
        value=str(bool(val)).lower(),
    )


@coerce_py_const.register(s_obj.get_known_type_id("std::uuid"))
def evaluate_std_uuid(
    type_id: uuid.UUID, val: Any
) -> irast.ConstExpr | irast.TypeCast:
    str_type_id = s_obj.get_known_type_id("std::str")
    str_typeref = irast.TypeRef(
        id=str_type_id, name_hint=sn.name_from_string("std::str")
    )
    return irast.TypeCast(
        from_type=str_typeref,
        to_type=irast.TypeRef(
            id=type_id, name_hint=sn.name_from_string("std::uuid")
        ),
        expr=irast.Set(
            expr=irast.StringConstant(typeref=str_typeref, value=str(val)),
            typeref=str_typeref,
            path_id=irast.PathId.from_typeref(str_typeref),
        ),
        sql_cast=True,
        sql_expr=False,
    )
