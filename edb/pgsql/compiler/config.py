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


from edb import errors
from edb.ir import ast as irast

from edb.pgsql import ast as pgast

from . import astutils
from . import context
from . import dispatch
from . import pathctx
from . import output


@dispatch.compile.register
def compile_ConfigSet(
        op: irast.ConfigSet, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:

    with ctx.new() as subctx:
        subctx.singleton_mode = True
        val = dispatch.compile(op.expr, ctx=subctx)
        val = output.serialize_expr(val, path_id=op.expr.path_id, env=ctx.env)

    result_row = pgast.RowExpr(
        args=[
            pgast.StringConstant(val='SET'),
            pgast.StringConstant(val='SYSTEM' if op.system else 'SESSION'),
            pgast.StringConstant(val=op.name),
            val,
        ]
    )

    result = pgast.FuncCall(
        name=('jsonb_build_array',),
        args=result_row.args,
        null_safe=True,
        ser_safe=True,
    )

    if not op.system:
        stmt = pgast.InsertStmt(
            relation=pgast.RangeVar(
                relation=pgast.Relation(
                    name='_edgecon_state',
                ),
            ),
            select_stmt=pgast.SelectStmt(
                values=[
                    pgast.ImplicitRowExpr(
                        args=[
                            pgast.StringConstant(
                                val=op.name,
                            ),
                            val,
                            pgast.StringConstant(
                                val='C',
                            ),
                        ]
                    )
                ]
            ),
            cols=[
                pgast.ColumnRef(name=['name']),
                pgast.ColumnRef(name=['value']),
                pgast.ColumnRef(name=['type']),
            ],
            on_conflict=pgast.OnConflictClause(
                action='update',
                infer=pgast.InferClause(
                    index_elems=[
                        pgast.ColumnRef(name=['name']),
                        pgast.ColumnRef(name=['type']),
                    ],
                ),
                target_list=[
                    pgast.MultiAssignRef(
                        columns=[pgast.ColumnRef(name=['value'])],
                        source=pgast.RowExpr(
                            args=[
                                val,
                            ],
                        ),
                    ),
                ],
            ),
        )
    else:
        stmt = pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=result,
                ),
            ],
        )

    return stmt


@dispatch.compile.register
def compile_ConfigReset(
        op: irast.ConfigReset, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:

    if not op.filter_properties:
        # Scalar reset
        result_row = pgast.RowExpr(
            args=[
                pgast.StringConstant(val='RESET'),
                pgast.StringConstant(val='SYSTEM' if op.system else 'SESSION'),
                pgast.StringConstant(val=op.name),
                pgast.NullConstant(),
            ]
        )
    else:
        # Composite reset
        args = []
        with ctx.new() as subctx:
            subctx.singleton_mode = True
            for propfilter in op.filter_properties:
                args.append(pgast.StringConstant(val=propfilter.property_name))
                args.append(dispatch.compile(propfilter.value, ctx=subctx))

        val = pgast.FuncCall(
            name=('jsonb_build_object',),
            args=args,
            null_safe=True,
            ser_safe=True,
        )

        result_row = pgast.RowExpr(
            args=[
                pgast.StringConstant(val='REM'),
                pgast.StringConstant(val='SYSTEM' if op.system else 'SESSION'),
                pgast.StringConstant(val=op.name),
                val,
            ]
        )

    result = pgast.FuncCall(
        name=('jsonb_build_array',),
        args=result_row.args,
        null_safe=True,
        ser_safe=True,
    )

    if not op.system:
        stmt = pgast.DeleteStmt(
            relation=pgast.RangeVar(
                relation=pgast.Relation(
                    name='_edgecon_state',
                ),
            ),

            where_clause=astutils.new_binop(
                lexpr=astutils.new_binop(
                    lexpr=pgast.ColumnRef(name=['name']),
                    rexpr=pgast.StringConstant(val=op.name),
                    op='=',
                ),
                rexpr=astutils.new_binop(
                    lexpr=pgast.ColumnRef(name=['type']),
                    rexpr=pgast.StringConstant(val='C'),
                    op='=',
                ),
                op='AND',
            )
        )
    else:
        stmt = pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=result,
                ),
            ],
        )

    return stmt


@dispatch.compile.register
def compile_ConfigInsert(
        stmt: irast.ConfigInsert, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:

    result_set = stmt.expr

    with ctx.new() as subctx:
        subctx.expr_exposed = True
        overlays = subctx.env.rel_overlays[
            str(result_set.typeref.material_type.id)]
        overlays.append(('replace', pgast.NullRelation(
            path_id=result_set.path_id)))
        dispatch.compile(result_set, ctx=subctx)

    return pathctx.get_path_serialized_output(
        ctx.rel, result_set.path_id, env=ctx.env)


def top_output_as_config_op(
        ir_set: irast.Set,
        stmt: pgast.Query, *,
        env: context.Environment) -> pgast.Query:

    if ir_set.expr.system:
        alias = env.aliases.get('cfg')
        subrvar = pgast.RangeSubselect(
            subquery=stmt,
            alias=pgast.Alias(
                aliasname=alias,
            )
        )

        stmt_res = stmt.target_list[0]

        if stmt_res.name is None:
            stmt_res = stmt.target_list[0] = pgast.ResTarget(
                name=env.aliases.get('v'),
                val=stmt_res.val,
            )

        result_row = pgast.RowExpr(
            args=[
                pgast.StringConstant(val='ADD'),
                pgast.StringConstant(
                    val='SYSTEM' if ir_set.expr.system else 'SESSION'),
                pgast.StringConstant(val=ir_set.expr.name),
                pgast.ColumnRef(name=[stmt_res.name]),
            ]
        )

        result = pgast.FuncCall(
            name=('jsonb_build_array',),
            args=result_row.args,
            null_safe=True,
            ser_safe=True,
        )

        return pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=result,
                ),
            ],
            from_clause=[
                subrvar,
            ],
        )

    else:
        raise errors.InternalServerError(
            'CONFIGURE SESSION INSERT is not supported')
