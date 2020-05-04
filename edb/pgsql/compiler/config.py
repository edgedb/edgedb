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


from __future__ import annotations

from edb import errors
from edb.ir import ast as irast

from edb.edgeql import qltypes

from edb.pgsql import ast as pgast

from . import astutils
from . import context
from . import dispatch
from . import pathctx
from . import relctx
from . import output


@dispatch.compile.register
def compile_ConfigSet(
        op: irast.ConfigSet, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:

    with ctx.new() as subctx:
        val = dispatch.compile(op.expr, ctx=subctx)
        assert isinstance(val, pgast.SelectStmt), "expected ast.SelectStmt"
        pathctx.get_path_serialized_output(
            val, op.expr.path_id, env=ctx.env)
        if op.cardinality is qltypes.SchemaCardinality.MANY:
            val = output.aggregate_json_output(val, op.expr, env=ctx.env)

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

    stmt: pgast.Query

    if not op.system:
        stmt = pgast.InsertStmt(
            relation=pgast.RelRangeVar(
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

    if op.selector is None:
        # Scalar reset
        result_row = pgast.RowExpr(
            args=[
                pgast.StringConstant(val='RESET'),
                pgast.StringConstant(val='SYSTEM' if op.system else 'SESSION'),
                pgast.StringConstant(val=op.name),
                pgast.NullConstant(),
            ]
        )

        rvar = None
    else:
        selector = dispatch.compile(op.selector, ctx=ctx)
        assert isinstance(selector, pgast.SelectStmt), \
            "expected ast.SelectStmt"
        target = selector.target_list[0]
        if not target.name:
            target = selector.target_list[0] = pgast.ResTarget(
                name=ctx.env.aliases.get('res'),
                val=target.val,
            )

        rvar = relctx.rvar_for_rel(selector, ctx=ctx)

        result_row = pgast.RowExpr(
            args=[
                pgast.StringConstant(val='REM'),
                pgast.StringConstant(val='SYSTEM' if op.system else 'SESSION'),
                pgast.StringConstant(val=op.name),
                astutils.get_column(rvar, target.name),
            ]
        )

    result = pgast.FuncCall(
        name=('jsonb_build_array',),
        args=result_row.args,
        null_safe=True,
        ser_safe=True,
    )

    stmt: pgast.Query

    if not op.system:
        stmt = pgast.DeleteStmt(
            relation=pgast.RelRangeVar(
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

        if rvar is not None:
            stmt.from_clause = [rvar]

    return stmt


@dispatch.compile.register
def compile_ConfigInsert(
        stmt: irast.ConfigInsert, *,
        ctx: context.CompilerContextLevel) -> pgast.Base:

    with ctx.new() as subctx:
        subctx.expr_exposed = True
        rewritten = _rewrite_config_insert(stmt.expr, ctx=subctx)
        dispatch.compile(rewritten, ctx=subctx)

    return pathctx.get_path_serialized_output(
        ctx.rel, stmt.expr.path_id, env=ctx.env)


def _rewrite_config_insert(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> irast.Set:

    relctx.add_type_rel_overlay(
        ir_set.typeref,
        'replace',
        pgast.NullRelation(path_id=ir_set.path_id),
        path_id=ir_set.path_id,
        ctx=ctx,
    )

    # Config objects have derived computed ids,
    # so the autogenerated id must not be returned.
    ir_set.shape = list(filter(
        lambda el: el[0].rptr.ptrref.shortname.name != 'id',
        ir_set.shape,
    ))

    for el, _ in ir_set.shape:
        if isinstance(el.expr, irast.InsertStmt):
            el.shape = list(filter(
                lambda e: e[0].rptr.ptrref.shortname.name != 'id',
                el.shape,
            ))

            result = _rewrite_config_insert(el.expr.subject, ctx=ctx)
            el.expr = irast.SelectStmt(
                result=result,
                parent_stmt=el.expr.parent_stmt,
            )

    return ir_set


def top_output_as_config_op(
        ir_set: irast.Set,
        stmt: pgast.SelectStmt, *,
        env: context.Environment) -> pgast.Query:

    assert isinstance(ir_set.expr, irast.ConfigCommand)

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
