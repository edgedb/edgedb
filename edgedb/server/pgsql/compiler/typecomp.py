##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""Functions dealing with PostgreSQL native types and casting."""


from edgedb.lang.common import ast
from edgedb.lang.schema import objects as s_obj

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import types as pg_types

from . import astutils
from . import context


def cast(
        node: pgast.Base, *,
        source_type: s_obj.Class, target_type: s_obj.Class,
        force: bool=False,
        env: context.Environment) -> pgast.Base:

    if source_type.name == target_type.name and not force:
        return node

    schema = env.schema

    if isinstance(target_type, s_obj.Collection):
        if target_type.schema_name == 'array':

            if source_type.name == 'std::json':
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
                # EdgeQL: <array<int>>['1', '2']
                # to SQL: ARRAY['1', '2']::int[]

                elem_pgtype = pg_types.pg_type_from_object(
                    schema, target_type.element_type, topbase=True)

                return pgast.TypeCast(
                    arg=node,
                    type_name=pgast.TypeName(
                        name=elem_pgtype,
                        array_bounds=[-1]))

        elif target_type.schema_name == 'map':
            if source_type.name == 'std::json':
                # If the source type is json do nothing, since
                # maps are already encoded in json.
                return node

            # EdgeQL: <map<Tkey,Tval>>MAP<Vkey,Vval>
            # to SQL: SELECT jsonb_object_agg(
            #                    key::Vkey::Tkey::text,
            #                    value::Vval::Tval)
            #         FROM jsonb_each_text(MAP)

            str_t = schema.get('std::str')

            key_cast = cast(
                cast(
                    cast(
                        pgast.ColumnRef(name=['key']),
                        source_type=str_t,
                        target_type=source_type.key_type,
                        env=env),
                    source_type=source_type.key_type,
                    target_type=target_type.key_type,
                    env=env,
                ),
                source_type=target_type.key_type,
                target_type=str_t,
                env=env,
            )

            target_v_type = target_type.element_type

            val_cast = cast(
                cast(
                    pgast.ColumnRef(name=['value']),
                    source_type=str_t,
                    target_type=source_type.element_type,
                    env=env),
                source_type=source_type.element_type,
                target_type=target_v_type,
                env=env
            )

            map_cast = pgast.SelectStmt(
                target_list=[
                    pgast.ResTarget(
                        val=pgast.FuncCall(
                            name=('jsonb_object_agg',),
                            args=[
                                key_cast,
                                val_cast
                            ])
                    )
                ],
                from_clause=[
                    pgast.RangeFunction(
                        functions=[pgast.FuncCall(
                            name=('jsonb_each_text',),
                            args=[
                                node
                            ]
                        )]
                    )
                ])

            return pgast.FuncCall(
                name=('coalesce',),
                args=[
                    map_cast,
                    pgast.TypeCast(
                        arg=pgast.Constant(val='{}'),
                        type_name=pgast.TypeName(
                            name=('jsonb',)
                        )
                    )
                ])

    else:
        # `target_type` is not a collection.
        if (source_type.name == 'std::datetime' and
                target_type.name == 'std::str'):
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

        elif (source_type.name == 'std::bool' and
                target_type.name == 'std::int'):
            # PostgreSQL 9.6 doesn't allow to cast 'boolean' to 'bigint':
            #      SELECT 'true'::boolean::bigint;
            #      ERROR:  cannot cast type boolean to bigint
            # So we transform EdgeQL: <int>BOOL
            # to SQL: BOOL::int::bigint
            return pgast.TypeCast(
                arg=pgast.TypeCast(
                    arg=node,
                    type_name=pgast.TypeName(name=('int',))),
                type_name=pgast.TypeName(name=('bigint',))
            )

        elif (source_type.name == 'std::int' and
                target_type.name == 'std::bool'):
            # PostgreSQL 9.6 doesn't allow to cast 'bigint' to 'boolean':
            #      SELECT 1::bigint::boolean;
            #      ERROR:  cannot cast type bigint to boolea
            # So we transform EdgeQL: <boolean>INT
            # to SQL: (INT != 0)
            return astutils.new_binop(
                node,
                pgast.Constant(val=0),
                op=ast.ops.NE)

        elif source_type.name == 'std::json':
            str_t = schema.get('std::str')

            if target_type.name in ('std::int', 'std::bool',
                                    'std::float'):
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

            elif target_type.name == 'std::str':
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
