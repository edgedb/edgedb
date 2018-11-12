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


"""Functions dealing with PostgreSQL native types and casting."""

import typing

from edb.lang.ir import ast as irast
from edb.lang.ir import utils as irutils

from edb.lang.common import ast
from edb.lang.schema import objects as s_obj

from edb.server.pgsql import ast as pgast
from edb.server.pgsql import types as pg_types

from . import astutils
from . import context


def cast(
        node: pgast.Base, *,
        source_type: s_obj.Object, target_type: s_obj.Object,
        ir_expr: typing.Optional[irast.Base]=None,
        force: bool=False,
        env: context.Environment) -> pgast.Base:

    if source_type.name == target_type.name and not force:
        return node

    schema = env.schema
    int_t = schema.get('std::anyint')
    json_t = schema.get('std::json')
    str_t = schema.get('std::str')
    datetime_t = schema.get('std::datetime')
    bool_t = schema.get('std::bool')
    real_t = schema.get('std::anyreal')
    bytes_t = schema.get('std::bytes')

    if target_type.is_array():
        if source_type.issubclass(env.schema, json_t):
            # If we are casting a jsonb array to array, we do the
            # following transformation:
            # EdgeQL: <array<T>>MAP_VALUE
            # SQL:
            #      SELECT array_agg(j::T)
            #      FROM jsonb_array_elements(MAP_VALUE) AS j

            inner_cast = cast(
                pgast.ColumnRef(name=['j']),
                source_type=source_type,
                target_type=target_type.element_type,
                env=env
            )

            return pgast.SelectStmt(
                target_list=[
                    pgast.ResTarget(
                        val=pgast.FuncCall(
                            name=('array_agg',),
                            args=[
                                inner_cast
                            ])
                    )
                ],
                from_clause=[
                    pgast.RangeFunction(
                        functions=[pgast.FuncCall(
                            name=('jsonb_array_elements',),
                            args=[
                                node
                            ]
                        )],
                        alias=pgast.Alias(
                            aliasname='j'
                        )
                    )
                ])

        else:
            # EdgeQL: <array<int64>>['1', '2']
            # to SQL: ARRAY['1', '2']::int[]

            elem_pgtype = pg_types.pg_type_from_object(
                schema, target_type.element_type, topbase=True)

            if elem_pgtype in {('anyelement',), ('anynonarray',)}:
                # We don't want to append '[]' suffix to
                # `anyelement` and `anynonarray`.
                return pgast.TypeCast(
                    arg=node,
                    type_name=pgast.TypeName(
                        name=('anyarray',)))

            else:
                return pgast.TypeCast(
                    arg=node,
                    type_name=pgast.TypeName(
                        name=elem_pgtype,
                        array_bounds=[-1]))

    elif target_type.is_tuple():
        if target_type.implicitly_castable_to(source_type, env.schema):
            return pgast.TypeCast(
                arg=node,
                type_name=pgast.TypeName(
                    name=('record',)))

    else:
        # `target_type` is not a collection.
        if (source_type.issubclass(env.schema, datetime_t) and
                target_type.issubclass(env.schema, str_t)):
            # Normalize datetime to text conversion to have the same
            # format as one would get by serializing to JSON.
            #
            # EdgeQL: <text><datetime>'2010-10-10';
            # To SQL: trim(to_json('2010-01-01'::timestamptz)::text, '"')
            return pgast.FuncCall(
                name=('trim',),
                args=[
                    pgast.TypeCast(
                        arg=pgast.FuncCall(
                            name=('to_json',),
                            args=[
                                node
                            ]),
                        type_name=pgast.TypeName(name=('text',))),
                    pgast.StringConstant(val='"')
                ])

        elif (source_type.issubclass(env.schema, bool_t) and
                target_type.issubclass(env.schema, int_t)):
            # PostgreSQL 9.6 doesn't allow to cast 'boolean' to any integer
            # other than int32:
            #      SELECT 'true'::boolean::bigint;
            #      ERROR:  cannot cast type boolean to bigint
            # So we transform EdgeQL: <int64>BOOL
            # to SQL: BOOL::int::<targetint>
            return pgast.TypeCast(
                arg=pgast.TypeCast(
                    arg=node,
                    type_name=pgast.TypeName(name=('int',))),
                type_name=pgast.TypeName(
                    name=pg_types.pg_type_from_scalar(schema, target_type))
            )

        elif (source_type.issubclass(env.schema, int_t) and
                target_type.issubclass(env.schema, bool_t)):
            # PostgreSQL 9.6 doesn't allow to cast any integer other
            # than int32 to 'boolean':
            #      SELECT 1::bigint::boolean;
            #      ERROR:  cannot cast type bigint to boolea
            # So we transform EdgeQL: <boolean>INT
            # to SQL: (INT != 0)
            return astutils.new_binop(
                node,
                pgast.NumericConstant(val='0'),
                op=ast.ops.NE)

        elif source_type.issubclass(env.schema, json_t):
            # When casting from json, we want the text representation
            # of the *value*, and not a JSON literal, so that
            # <str><json>'foo' returns 'foo', and not '"foo"'.
            # Hence, instead of a direct cast, we use the '->>' operator
            # on an intermediate array container.

            const_type = pg_types.pg_type_from_object(
                schema, target_type, topbase=True)

            if target_type.issubclass(env.schema, real_t):
                expected_json_type = 'number'
            elif target_type.issubclass(env.schema, bool_t):
                expected_json_type = 'boolean'
            elif target_type.issubclass(env.schema, str_t):
                expected_json_type = 'string'
            elif target_type.issubclass(env.schema, json_t):
                expected_json_type = None
            else:
                raise NotImplementedError(
                    f'cannot not cast {source_type.name} to {target_type.name}'
                )

            if expected_json_type is not None:
                if ir_expr is not None:
                    srcctx = irutils.get_source_context_as_json(ir_expr)
                    details = pgast.StringConstant(val=srcctx)
                else:
                    srcctx = None
                    details = pgast.NullConstant()

                node = pgast.FuncCall(
                    name=('edgedb', 'jsonb_assert_type'),
                    args=[
                        node,
                        pgast.ArrayExpr(elements=[
                            pgast.StringConstant(val=expected_json_type),
                            pgast.StringConstant(val='null'),
                        ]),
                        pgast.NamedFuncArg(
                            name='det',
                            val=details,
                        )
                    ]
                )

            return pgast.TypeCast(
                arg=astutils.new_binop(
                    lexpr=pgast.FuncCall(
                        name=('array_to_json',),
                        args=[pgast.ArrayExpr(elements=[node])]
                    ),
                    rexpr=pgast.NumericConstant(val='0'),
                    op='->>'
                ),
                type_name=pgast.TypeName(
                    name=const_type
                )
            )

        elif target_type.issubclass(env.schema, json_t):
            if source_type.issubclass(env.schema, bytes_t):
                raise TypeError('cannot cast bytes to json')
            return pgast.FuncCall(
                name=('to_jsonb',), args=[node])
        else:
            const_type = pg_types.pg_type_from_object(
                schema, target_type, topbase=True)

            return pgast.TypeCast(
                arg=node,
                type_name=pgast.TypeName(
                    name=const_type
                )
            )

    raise NotImplementedError(
        f'cannot not cast {source_type.name} to {target_type.name}'
    )


def type_node(typename):
    typename = list(typename)
    if typename[-1].endswith('[]'):
        # array
        typename[-1] = typename[-1][:-2]
        array_bounds = [-1]
    else:
        array_bounds = []

    return pgast.TypeName(
        name=tuple(typename),
        array_bounds=array_bounds
    )
