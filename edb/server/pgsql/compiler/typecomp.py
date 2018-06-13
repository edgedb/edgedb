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


from edb.lang.common import ast
from edb.lang.schema import objects as s_obj
from edb.lang.schema import types as s_types

from edb.server.pgsql import ast as pgast
from edb.server.pgsql import types as pg_types

from . import astutils
from . import context


def cast(
        node: pgast.Base, *,
        source_type: s_obj.Object, target_type: s_obj.Object,
        force: bool=False,
        env: context.Environment) -> pgast.Base:

    if source_type.name == target_type.name and not force:
        return node

    schema = env.schema
    real_t = schema.get('std::anyreal')
    int_t = schema.get('std::anyint')
    json_t = schema.get('std::json')
    str_t = schema.get('std::str')
    datetime_t = schema.get('std::datetime')
    bool_t = schema.get('std::bool')

    if isinstance(target_type, s_types.Collection):
        if target_type.schema_name == 'array':

            if source_type.issubclass(json_t):
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

                return pgast.TypeCast(
                    arg=node,
                    type_name=pgast.TypeName(
                        name=elem_pgtype,
                        array_bounds=[-1]))

    else:
        # `target_type` is not a collection.
        if (source_type.issubclass(datetime_t) and
                target_type.issubclass(str_t)):
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
                    pgast.Constant(val='"')
                ])

        elif source_type.issubclass(bool_t) and target_type.issubclass(int_t):
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

        elif source_type.issubclass(int_t) and target_type.issubclass(bool_t):
            # PostgreSQL 9.6 doesn't allow to cast any integer other
            # than int32 to 'boolean':
            #      SELECT 1::bigint::boolean;
            #      ERROR:  cannot cast type bigint to boolea
            # So we transform EdgeQL: <boolean>INT
            # to SQL: (INT != 0)
            return astutils.new_binop(
                node,
                pgast.Constant(val=0),
                op=ast.ops.NE)

        elif source_type.issubclass(json_t):
            if (target_type.issubclass(real_t) or
                    target_type.issubclass(bool_t)):
                # Simply cast to text and the to the target type.
                return cast(
                    cast(
                        node,
                        source_type=source_type,
                        target_type=str_t,
                        env=env),
                    source_type=str_t,
                    target_type=target_type,
                    env=env)

            elif target_type.issubclass(str_t):
                # It's not possible to cast jsonb string to text directly,
                # so we do a trick:
                # EdgeQL: <str>JSONB_VAL
                # SQL: array_to_json(ARRAY[JSONB_VAL])->>0

                return astutils.new_binop(
                    pgast.FuncCall(
                        name=('array_to_json',),
                        args=[pgast.ArrayExpr(elements=[node])]),
                    pgast.Constant(val=0),
                    op='->>'
                )

            elif target_type.issubclass(json_t):
                return pgast.TypeCast(
                    arg=node,
                    type_name=pgast.TypeName(
                        name=('jsonb',)
                    )
                )

        else:
            const_type = pg_types.pg_type_from_object(
                schema, target_type, topbase=True)

            return pgast.TypeCast(
                arg=node,
                type_name=pgast.TypeName(
                    name=const_type
                )
            )

    raise RuntimeError(
        f'could not cast {source_type.name} to {target_type.name}')


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
