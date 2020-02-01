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


from __future__ import annotations

import functools
from typing import *  # NoQA

from edb import errors

from edb.schema import abc as s_abc
from edb.schema import name as s_name
from edb.schema import objects as s_obj
from edb.schema import objtypes as s_objtypes
from edb.schema import pseudo as s_pseudo
from edb.schema import scalars as s_scalars
from edb.schema import types as s_types
from edb.schema import utils as s_utils

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils

from .. import context


def amend_empty_set_type(
    es: irast.EmptySet,
    t: s_types.Type,
    env: context.Environment
) -> None:
    env.set_types[es] = t
    alias = es.path_id.target_name_hint.name
    typename = s_name.Name(module='__derived__', name=alias)
    es.path_id = irast.PathId.from_type(
        env.schema, t, env=env, typename=typename
    )


def _infer_common_type(
    irs: List[irast.Base],
    env: context.Environment
) -> Optional[s_types.Type]:
    if not irs:
        raise errors.QueryError(
            'cannot determine common type of an empty set',
            context=irs[0].context)

    types = []
    empties = []

    seen_object = False
    seen_scalar = False
    seen_coll = False

    for i, arg in enumerate(irs):
        if isinstance(arg, irast.EmptySet) and env.set_types[arg] is None:
            empties.append(i)
            continue

        t = infer_type(arg, env)
        if isinstance(t, s_abc.Collection):
            seen_coll = True
        elif isinstance(t, s_scalars.ScalarType):
            seen_scalar = True
        else:
            seen_object = True
        types.append(t)

    if seen_coll + seen_scalar + seen_object > 1:
        raise errors.QueryError(
            'cannot determine common type',
            context=irs[0].context)

    if not types:
        raise errors.QueryError(
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
        amend_empty_set_type(
            cast(irast.EmptySet, irs[i]), common_type, env)

    return common_type


@functools.singledispatch
def _infer_type(
    ir: irast.Base,
    env: context.Environment,
) -> s_types.Type:
    raise ValueError(f'infer_cardinality: cannot handle {ir!r}')


@_infer_type.register(type(None))
def __infer_none(
    ir: None,
    env: context.Environment,
) -> s_types.Type:
    # Here for debugging purposes.
    raise ValueError('invalid infer_type(None, env) call')


@_infer_type.register
def __infer_statement(
    ir: irast.Statement,
    env: context.Environment,
) -> s_types.Type:
    return infer_type(ir.expr, env)


@_infer_type.register
def __infer_set(
    ir: irast.Set,
    env: context.Environment,
) -> s_types.Type:
    return env.set_types[ir]


@_infer_type.register
def __infer_type_introspection(
    ir: irast.TypeIntrospection,
    env: context.Environment,
) -> s_types.Type:
    if irtyputils.is_scalar(ir.typeref):
        return cast(s_objtypes.ObjectType,
                    env.schema.get('schema::ScalarType'))
    elif irtyputils.is_object(ir.typeref):
        return cast(s_objtypes.ObjectType,
                    env.schema.get('schema::ObjectType'))
    elif irtyputils.is_array(ir.typeref):
        return cast(s_objtypes.ObjectType,
                    env.schema.get('schema::Array'))
    elif irtyputils.is_tuple(ir.typeref):
        return cast(s_objtypes.ObjectType,
                    env.schema.get('schema::Tuple'))
    else:
        raise errors.QueryError(
            'unexpected type in INTROSPECT', context=ir.context)


@_infer_type.register
def __infer_func_call(
    ir: irast.FunctionCall,
    env: context.Environment,
) -> s_types.Type:
    return irtyputils.ir_typeref_to_type(env.schema, ir.typeref)


@_infer_type.register
def __infer_oper_call(
    ir: irast.OperatorCall,
    env: context.Environment,
) -> s_types.Type:
    return irtyputils.ir_typeref_to_type(env.schema, ir.typeref)


@_infer_type.register
def __infer_const(
    ir: irast.BaseConstant,
    env: context.Environment,
) -> s_types.Type:
    return irtyputils.ir_typeref_to_type(env.schema, ir.typeref)


@_infer_type.register
def __infer_const_set(
    ir: irast.ConstantSet,
    env: context.Environment,
) -> s_types.Type:
    return irtyputils.ir_typeref_to_type(env.schema, ir.typeref)


@_infer_type.register
def __infer_param(
    ir: irast.Parameter,
    env: context.Environment,
) -> s_types.Type:
    return irtyputils.ir_typeref_to_type(env.schema, ir.typeref)


def _infer_binop_args(
    left: irast.Base,
    right: irast.Base,
    env: context.Environment
) -> Tuple[s_types.Type, s_types.Type]:

    if isinstance(left, irast.EmptySet):
        inferred_left_type = None
    else:
        inferred_left_type = infer_type(left, env)

    if isinstance(right, irast.EmptySet):
        inferred_right_type = None
    else:
        inferred_right_type = infer_type(right, env)

    if inferred_right_type is not None:
        if isinstance(left, irast.EmptySet):
            amend_empty_set_type(left, inferred_right_type, env)
        left_type = right_type = inferred_right_type
    elif inferred_left_type is not None:
        if isinstance(right, irast.EmptySet):
            amend_empty_set_type(right, inferred_left_type, env)
        left_type = right_type = inferred_left_type
    else:
        raise errors.QueryError(
            'cannot determine the type of an empty set',
            context=left.context)

    return left_type, right_type


@_infer_type.register
def __infer_typecheckop(
    ir: irast.TypeCheckOp,
    env: context.Environment,
) -> s_types.Type:
    left_type, right_type = _infer_binop_args(ir.left, ir.right, env)
    return cast(s_scalars.ScalarType, env.schema.get('std::bool'))


@_infer_type.register
def __infer_anytyperef(
    ir: irast.AnyTypeRef,
    env: context.Environment,
) -> s_types.Type:
    return s_pseudo.Any.instance()


@_infer_type.register
def __infer_anytupleref(
    ir: irast.AnyTupleRef,
    env: context.Environment,
) -> s_types.Type:
    return s_pseudo.AnyTuple.instance()


@_infer_type.register
def __infer_typeref(
    ir: irast.TypeRef,
    env: context.Environment,
) -> s_types.Type:
    result: s_types.Type

    if ir.collection:
        coll = s_types.Collection.get_class(ir.collection)
        if issubclass(coll, s_types.Tuple):
            named = False
            if any(t.element_name for t in ir.subtypes):
                named = True

            if named:
                eltypes = {st.element_name: infer_type(st, env)
                           for st in ir.subtypes}
            else:
                eltypes = {str(i): infer_type(st, env)
                           for i, st in enumerate(ir.subtypes)}

            result = coll.create(
                env.schema, element_types=eltypes, named=named)
        else:
            result = coll.from_subtypes(
                env.schema, [infer_type(t, env) for t in ir.subtypes])
    else:
        t = env.schema.get_by_id(ir.id)
        assert isinstance(t, s_types.Type)
        result = t

    return result


@_infer_type.register
def __infer_typecast(
    ir: irast.TypeCast,
    env: context.Environment,
) -> s_types.Type:
    stype = infer_type(ir.to_type, env)

    # is_polymorphic is synonymous to get_is_abstract for scalars
    if stype.is_polymorphic(env.schema):
        raise errors.QueryError(
            f'cannot cast into generic type '
            f'{stype.get_displayname(env.schema)!r}',
            context=ir.context)

    return stype


@_infer_type.register
def __infer_stmt(
    ir: irast.Stmt,
    env: context.Environment,
) -> s_types.Type:
    return infer_type(ir.result, env)


@_infer_type.register
def __infer_config_insert(
    ir: irast.ConfigInsert,
    env: context.Environment,
) -> s_types.Type:
    return infer_type(ir.expr, env)


@_infer_type.register
def __infer_slice(
    ir: irast.SliceIndirection,
    env: context.Environment,
) -> s_types.Type:
    node_type = infer_type(ir.expr, env)

    str_t = cast(s_scalars.ScalarType, env.schema.get('std::str'))
    int_t = cast(s_scalars.ScalarType, env.schema.get('std::int64'))
    json_t = cast(s_scalars.ScalarType, env.schema.get('std::json'))
    bytes_t = cast(s_scalars.ScalarType, env.schema.get('std::bytes'))

    if node_type.issubclass(env.schema, str_t):
        base_name = 'string'
    elif node_type.issubclass(env.schema, json_t):
        base_name = 'json array'
    elif node_type.issubclass(env.schema, bytes_t):
        base_name = 'bytes'
    elif isinstance(node_type, s_abc.Array):
        base_name = 'array'
    elif node_type.is_any():
        base_name = 'anytype'
    else:
        # the base type is not valid
        raise errors.QueryError(
            f'{node_type.get_verbosename(env.schema)} cannot be sliced',
            context=ir.start.context)

    for index in [ir.start, ir.stop]:
        if index is not None:
            index_type = infer_type(index, env)

            if not index_type.implicitly_castable_to(int_t, env.schema):
                raise errors.QueryError(
                    f'cannot slice {base_name} by '
                    f'{index_type.get_displayname(env.schema)}, '
                    f'{int_t.get_displayname(env.schema)} was expected',
                    context=index.context)

    return node_type


@_infer_type.register
def __infer_index(
    ir: irast.IndexIndirection,
    env: context.Environment,
) -> s_types.Type:
    node_type = infer_type(ir.expr, env)
    index_type = infer_type(ir.index, env)

    str_t = cast(s_scalars.ScalarType, env.schema.get('std::str'))
    bytes_t = cast(s_scalars.ScalarType, env.schema.get('std::bytes'))
    int_t = cast(s_scalars.ScalarType, env.schema.get('std::int64'))
    json_t = cast(s_scalars.ScalarType, env.schema.get('std::json'))

    result: s_types.Type

    if node_type.issubclass(env.schema, str_t):

        if not index_type.implicitly_castable_to(int_t, env.schema):
            raise errors.QueryError(
                f'cannot index string by '
                f'{index_type.get_displayname(env.schema)}, '
                f'{int_t.get_displayname(env.schema)} was expected',
                context=ir.index.context)

        result = str_t

    elif node_type.issubclass(env.schema, bytes_t):

        if not index_type.implicitly_castable_to(int_t, env.schema):
            raise errors.QueryError(
                f'cannot index bytes by '
                f'{index_type.get_displayname(env.schema)}, '
                f'{int_t.get_displayname(env.schema)} was expected',
                context=ir.index.context)

        result = bytes_t

    elif node_type.issubclass(env.schema, json_t):

        if not (index_type.implicitly_castable_to(int_t, env.schema) or
                index_type.implicitly_castable_to(str_t, env.schema)):

            raise errors.QueryError(
                f'cannot index json by '
                f'{index_type.get_displayname(env.schema)}, '
                f'{int_t.get_displayname(env.schema)} or '
                f'{str_t.get_displayname(env.schema)} was expected',
                context=ir.index.context)

        result = json_t

    elif isinstance(node_type, s_abc.Array):

        if not index_type.implicitly_castable_to(int_t, env.schema):
            raise errors.QueryError(
                f'cannot index array by '
                f'{index_type.get_displayname(env.schema)}, '
                f'{int_t.get_displayname(env.schema)} was expected',
                context=ir.index.context)

        result = node_type.get_subtypes(env.schema)[0]

    elif (node_type.is_any() or
            (node_type.is_scalar() and
                node_type.get_name(env.schema) == 'std::anyscalar') and
            (index_type.implicitly_castable_to(int_t, env.schema) or
                index_type.implicitly_castable_to(str_t, env.schema))):
        result = s_pseudo.Any.instance()

    else:
        raise errors.QueryError(
            f'index indirection cannot be applied to '
            f'{node_type.get_verbosename(env.schema)}',
            context=ir.index.context)

    return result


@_infer_type.register
def __infer_array(
    ir: irast.Array,
    env: context.Environment,
) -> s_types.Type:
    if ir.typeref is not None:
        return irtyputils.ir_typeref_to_type(env.schema, ir.typeref)
    elif ir.elements:
        element_type = _infer_common_type(ir.elements, env)
        if element_type is None:
            raise errors.QueryError('could not determine array type',
                                    context=ir.context)
    else:
        element_type = s_pseudo.Any.instance()

    return s_types.Array.create(env.schema, element_type=element_type)


@_infer_type.register
def __infer_tuple(
    ir: irast.Tuple,
    env: context.Environment,
) -> s_types.Type:
    element_types = {el.name: infer_type(el.val, env) for el in ir.elements}
    return s_types.Tuple.create(
        env.schema, element_types=element_types, named=ir.named)


@_infer_type.register
def __infer_tuple_indirection(
    ir: irast.TupleIndirection,
    env: context.Environment,
) -> s_types.Type:
    tuple_type = infer_type(ir.expr, env)
    assert isinstance(tuple_type, s_types.Tuple)
    result = tuple_type.get_subtype(env.schema, ir.name)
    if result is None:
        raise errors.QueryError('could not determine struct element type',
                                context=ir.context)

    return result


def infer_type(ir: irast.Base, env: context.Environment) -> s_types.Type:
    result = env.inferred_types.get(ir)
    if result is not None:
        return result

    result = _infer_type(ir, env)

    if (result is not None and
            not isinstance(result, (s_obj.Object, s_obj.ObjectMeta))):

        raise errors.QueryError(
            f'infer_type({ir!r}) retured {result!r} instead of a Object',
            context=ir.context)

    if result is None:
        raise errors.QueryError(
            'could not determine expression type',
            context=ir.context)

    env.inferred_types[ir] = result

    return result
