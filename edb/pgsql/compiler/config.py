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
from edb.ir import typeutils as irtyputils

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from edb.schema import casts as s_casts
from edb.schema import name as sn

from edb.pgsql import ast as pgast
from edb.pgsql import common

from . import astutils
from . import context
from . import dispatch
from . import pathctx
from . import relctx
from . import output


@dispatch.compile.register
def compile_ConfigSet(
    op: irast.ConfigSet,
    *,
    ctx: context.CompilerContextLevel,
) -> pgast.BaseExpr:

    val = _compile_config_value(op, ctx=ctx)
    result: pgast.BaseExpr

    if op.scope is qltypes.ConfigScope.INSTANCE and op.backend_setting:
        if not ctx.env.backend_runtime_params.has_configfile_access:
            raise errors.UnsupportedBackendFeatureError(
                "configuring backend parameters via CONFIGURE INSTANCE"
                " is not supported by the current backend"
            )
        result = pgast.AlterSystem(
            name=op.backend_setting,
            value=val,
        )

    elif op.scope is qltypes.ConfigScope.DATABASE and op.backend_setting:
        if not isinstance(val, pgast.StringConstant):
            val = pgast.TypeCast(
                arg=val,
                type_name=pgast.TypeName(name=('text',)),
            )

        fcall = pgast.FuncCall(
            name=('edgedb', '_alter_current_database_set'),
            args=[pgast.StringConstant(val=op.backend_setting), val],
        )

        result = output.wrap_script_stmt(
            pgast.SelectStmt(target_list=[pgast.ResTarget(val=fcall)]),
            suppress_all_output=True,
            env=ctx.env,
        )

    elif op.scope is qltypes.ConfigScope.SESSION and op.backend_setting:
        if not isinstance(val, pgast.StringConstant):
            val = pgast.TypeCast(
                arg=val,
                type_name=pgast.TypeName(name=('text',)),
            )

        fcall = pgast.FuncCall(
            name=('pg_catalog', 'set_config'),
            args=[
                pgast.StringConstant(val=op.backend_setting),
                val,
                pgast.BooleanConstant(val=False),
            ],
        )

        result = output.wrap_script_stmt(
            pgast.SelectStmt(target_list=[pgast.ResTarget(val=fcall)]),
            suppress_all_output=True,
            env=ctx.env,
        )

    elif op.scope is qltypes.ConfigScope.INSTANCE:
        result_row = pgast.RowExpr(
            args=[
                pgast.StringConstant(val='SET'),
                pgast.StringConstant(val=str(op.scope)),
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

        result = pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=result,
                ),
            ],
        )
    elif op.scope is qltypes.ConfigScope.SESSION:
        result = pgast.InsertStmt(
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
                pgast.InsertTarget(name='name'),
                pgast.InsertTarget(name='value'),
                pgast.InsertTarget(name='type'),
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

    elif op.scope is qltypes.ConfigScope.GLOBAL:
        result_row = pgast.RowExpr(
            args=[
                pgast.StringConstant(val='SET'),
                pgast.StringConstant(val=str(op.scope)),
                pgast.StringConstant(val=op.name),
                val,
            ]
        )

        build_array = pgast.FuncCall(
            name=('jsonb_build_array',),
            args=result_row.args,
            null_safe=True,
            ser_safe=True,
        )

        result = pgast.SelectStmt(
            target_list=[pgast.ResTarget(val=build_array)],
        )

    elif op.scope is qltypes.ConfigScope.DATABASE:
        result = pgast.InsertStmt(
            relation=pgast.RelRangeVar(
                relation=pgast.Relation(
                    name='_db_config',
                    schemaname='edgedb',
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
                        ]
                    )
                ]
            ),
            cols=[
                pgast.InsertTarget(name='name'),
                pgast.InsertTarget(name='value'),
            ],
            on_conflict=pgast.OnConflictClause(
                action='update',
                infer=pgast.InferClause(
                    index_elems=[
                        pgast.ColumnRef(name=['name']),
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
        raise AssertionError(f'unexpected configuration scope: {op.scope}')

    return result


@dispatch.compile.register
def compile_ConfigReset(
    op: irast.ConfigReset,
    *,
    ctx: context.CompilerContextLevel,
) -> pgast.BaseExpr:

    stmt: pgast.BaseExpr

    if op.scope is qltypes.ConfigScope.INSTANCE and op.backend_setting:
        stmt = pgast.AlterSystem(
            name=op.backend_setting,
            value=None,
        )

    elif op.scope is qltypes.ConfigScope.DATABASE and op.backend_setting:
        fcall = pgast.FuncCall(
            name=('edgedb', '_alter_current_database_set'),
            args=[
                pgast.StringConstant(val=op.backend_setting),
                pgast.NullConstant(),
            ],
        )

        stmt = output.wrap_script_stmt(
            pgast.SelectStmt(target_list=[pgast.ResTarget(val=fcall)]),
            suppress_all_output=True,
            env=ctx.env,
        )

    elif op.scope is qltypes.ConfigScope.SESSION and op.backend_setting:
        fcall = pgast.FuncCall(
            name=('pg_catalog', 'set_config'),
            args=[
                pgast.StringConstant(val=op.backend_setting),
                pgast.NullConstant(),
                pgast.BooleanConstant(val=False),
            ],
        )

        stmt = output.wrap_script_stmt(
            pgast.SelectStmt(target_list=[pgast.ResTarget(val=fcall)]),
            suppress_all_output=True,
            env=ctx.env,
        )

    elif op.scope is qltypes.ConfigScope.INSTANCE:

        if op.selector is None:
            # Scalar reset
            result_row = pgast.RowExpr(
                args=[
                    pgast.StringConstant(val='RESET'),
                    pgast.StringConstant(val=str(op.scope)),
                    pgast.StringConstant(val=op.name),
                    pgast.NullConstant(),
                ]
            )

            rvar = None
        else:
            with context.output_format(ctx, context.OutputFormat.JSONB):
                selector = dispatch.compile(op.selector, ctx=ctx)

            assert isinstance(selector, pgast.SelectStmt), \
                "expected ast.SelectStmt"
            target = selector.target_list[0]
            if not target.name:
                target = selector.target_list[0] = pgast.ResTarget(
                    name=ctx.env.aliases.get('res'),
                    val=target.val,
                )
                assert target.name is not None

            rvar = relctx.rvar_for_rel(selector, ctx=ctx)

            result_row = pgast.RowExpr(
                args=[
                    pgast.StringConstant(val='REM'),
                    pgast.StringConstant(val=str(op.scope)),
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

        stmt = pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=result,
                ),
            ],
        )

        if rvar is not None:
            stmt.from_clause = [rvar]

    elif op.scope is qltypes.ConfigScope.DATABASE and op.selector is None:
        stmt = pgast.DeleteStmt(
            relation=pgast.RelRangeVar(
                relation=pgast.Relation(
                    name='_db_config',
                    schemaname='edgedb',
                ),
            ),

            where_clause=astutils.new_binop(
                lexpr=pgast.ColumnRef(name=['name']),
                rexpr=pgast.StringConstant(val=op.name),
                op='=',
            ),
        )

    elif op.scope is qltypes.ConfigScope.DATABASE and op.selector is not None:
        # For FILTERed RESET on the database, we have to do a decent
        # amount of work to actually delete the RESET objects from the
        # json config blogs.
        #
        # This is because the server isn't set up to write back just
        # the changed parts of the config based on interpreting the output,
        # so instead we do all the work here.
        with context.output_format(ctx, context.OutputFormat.JSONB):
            selector = dispatch.compile(op.selector, ctx=ctx)

        assert isinstance(selector, pgast.SelectStmt), \
            "expected ast.SelectStmt"
        target = selector.target_list[0]
        if not target.name:
            target = selector.target_list[0] = pgast.ResTarget(
                name=ctx.env.aliases.get('res'),
                val=target.val,
            )
            assert target.name is not None

        rvar = relctx.rvar_for_rel(selector, ctx=ctx)

        sel_expr = op.selector.expr
        assert isinstance(sel_expr, irast.SelectStmt)
        sel_expr = sel_expr.result.expr
        assert isinstance(sel_expr, irast.SelectStmt)

        # Grab all the non-link properties of the object as keys. We
        # could just do the exclusive ones, but this works too and we
        # have the information at hand.
        # XXX: Do we need to consider _tname also?
        keys = [
            el.expr.ptrref.shortname.name
            for el, op in sel_expr.result.shape
            if op == qlast.ShapeOp.ASSIGN
            and not irtyputils.is_object(el.expr.ptrref.out_target)
        ]

        newval = pgast.SelectStmt(
            target_list=[pgast.ResTarget(
                val=pgast.FuncCall(
                    name=('jsonb_agg',),
                    args=[pgast.ColumnRef(name=['ov', 'value'])],
                ),
            )],
            from_clause=[
                pgast.RangeFunction(
                    lateral=True,
                    alias=pgast.Alias(aliasname='ov'),
                    functions=[pgast.FuncCall(
                        name=('jsonb_array_elements',),
                        args=[pgast.ColumnRef(name=['value'])],
                    )],
                ),
            ],
            where_clause=(
                pgast.SubLink(
                    operator="NOT EXISTS",
                    expr=pgast.SelectStmt(
                        from_clause=[rvar],
                        where_clause=astutils.extend_binop(
                            None,
                            *[
                                pgast.Expr(
                                    name='=',
                                    lexpr=pgast.Expr(
                                        name='->',
                                        lexpr=pgast.ColumnRef(name=[
                                            rvar.alias.aliasname,
                                            target.name,
                                        ]),
                                        rexpr=pgast.StringConstant(val=key),
                                    ),
                                    rexpr=pgast.CoalesceExpr(
                                        args=[
                                            pgast.Expr(
                                                name='->',
                                                lexpr=pgast.ColumnRef(name=[
                                                    'ov', 'value'
                                                ]),
                                                rexpr=pgast.StringConstant(
                                                    val=key
                                                ),
                                            ),
                                            pgast.TypeCast(
                                                arg=pgast.StringConstant(
                                                    val='null'),
                                                type_name=pgast.TypeName(
                                                    name=('jsonb',),
                                                ),
                                            ),
                                        ]
                                    )
                                )
                                for key in keys
                            ],
                        )
                    )
                )
            ),
        )

        stmt = pgast.UpdateStmt(
            targets=[pgast.UpdateTarget(
                name='value',
                val=newval,
            )],
            relation=pgast.RelRangeVar(
                relation=pgast.Relation(
                    name='_db_config',
                    schemaname='edgedb',
                ),
            ),
            where_clause=astutils.new_binop(
                lexpr=pgast.ColumnRef(name=['name']),
                rexpr=pgast.StringConstant(val=op.name),
                op='=',
            ),
            returning_list=[pgast.ResTarget(
                val=pgast.CaseExpr(
                    args=[
                        pgast.CaseWhen(
                            expr=pgast.NullTest(
                                arg=pgast.ColumnRef(name=['value'])
                            ),
                            result=pgast.FuncCall(
                                name=('jsonb_build_array',),
                                args=[
                                    pgast.StringConstant(val='RESET'),
                                    pgast.StringConstant(val=str(op.scope)),
                                    pgast.StringConstant(val=op.name),
                                    pgast.NullConstant(),
                                ],
                            )
                        ),
                    ],
                    defresult=pgast.FuncCall(
                        name=('jsonb_build_array',),
                        args=[
                            pgast.StringConstant(val='SET'),
                            pgast.StringConstant(val=str(op.scope)),
                            pgast.StringConstant(val=op.name),
                            pgast.ColumnRef(name=['value']),
                        ],
                    )
                )
            )],
        )

    elif op.scope is qltypes.ConfigScope.SESSION:
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
    elif op.scope is qltypes.ConfigScope.GLOBAL:
        stmt = pgast.SelectStmt(
            where_clause=pgast.BooleanConstant(val=False)
        )
    else:
        raise AssertionError(f'unexpected configuration scope: {op.scope}')

    return stmt


@dispatch.compile.register
def compile_ConfigInsert(
        stmt: irast.ConfigInsert, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:

    with ctx.new() as subctx:
        with context.output_format(ctx, context.OutputFormat.JSONB):
            subctx.expr_exposed = True
            rewritten = _rewrite_config_insert(stmt.expr, ctx=subctx)
            dispatch.compile(rewritten, ctx=subctx)

            return pathctx.get_path_serialized_output(
                ctx.rel, stmt.expr.path_id, env=ctx.env)


def _rewrite_config_insert(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> irast.Set:

    overwrite_query = pgast.SelectStmt()
    id_expr = pgast.FuncCall(
        name=('edgedb', 'uuid_generate_v1mc'),
        args=[],
    )
    pathctx.put_path_identity_var(
        overwrite_query, ir_set.path_id, id_expr, force=True
    )
    pathctx.put_path_value_var(
        overwrite_query, ir_set.path_id, id_expr, force=True
    )
    pathctx.put_path_source_rvar(
        overwrite_query,
        ir_set.path_id,
        relctx.rvar_for_rel(pgast.NullRelation(), ctx=ctx),
    )

    relctx.add_type_rel_overlay(
        ir_set.typeref,
        'replace',
        overwrite_query,
        path_id=ir_set.path_id,
        ctx=ctx,
    )

    # Config objects have derived computed ids,
    # so the autogenerated id must not be returned.
    ir_set.shape = tuple(filter(
        lambda el: (
            el[0].expr.ptrref.shortname.name != 'id'
        ),
        ir_set.shape,
    ))

    for el, _ in ir_set.shape:
        if isinstance(el.expr.expr, irast.InsertStmt):
            el.shape = tuple(filter(
                lambda e: (
                    e[0].expr.ptrref.shortname.name != 'id'
                ),
                el.shape,
            ))

            result = _rewrite_config_insert(el.expr.expr.subject, ctx=ctx)
            el.expr.expr = irast.SelectStmt(
                result=result,
                parent_stmt=el.expr.expr.parent_stmt,
            )

    return ir_set


def _compile_config_value(
    op: irast.ConfigSet,
    *,
    ctx: context.CompilerContextLevel,
) -> pgast.BaseExpr:
    val: pgast.BaseExpr

    expr = op.backend_expr or op.expr

    with ctx.new() as subctx:
        if op.backend_setting or op.scope == qltypes.ConfigScope.GLOBAL:
            output_format = context.OutputFormat.NATIVE
        else:
            output_format = context.OutputFormat.JSONB

        with context.output_format(ctx, output_format):
            if isinstance(expr, irast.EmptySet):
                # Special handling for empty sets, because we want a
                # singleton representation of the value and not an empty rel
                # in this context.
                if op.cardinality is qltypes.SchemaCardinality.One:
                    val = pgast.NullConstant()
                elif subctx.env.output_format is context.OutputFormat.JSONB:
                    val = pgast.TypeCast(
                        arg=pgast.StringConstant(val='[]'),
                        type_name=pgast.TypeName(
                            name=('jsonb',),
                        ),
                    )
                else:
                    val = pgast.TypeCast(
                        arg=pgast.ArrayExpr(elements=[]),
                        type_name=pgast.TypeName(
                            name=('text[]',),
                        ),
                    )
            else:
                val = dispatch.compile(expr, ctx=subctx)
                assert isinstance(val, pgast.SelectStmt), "expected SelectStmt"

                pathctx.get_path_serialized_output(
                    val, expr.path_id, env=ctx.env)

                if op.cardinality is qltypes.SchemaCardinality.Many:
                    val = output.aggregate_json_output(
                        val, expr, env=ctx.env)

    # For globals, we need to output the binary encoding so that we
    # can just hand it back to the server. We abuse `record_send` to
    # act as a generic `_send` function
    if op.scope is qltypes.ConfigScope.GLOBAL:
        val = pgast.FuncCall(
            name=('substring',),
            args=[
                pgast.FuncCall(
                    name=('record_send',),
                    args=[pgast.RowExpr(args=[val])],
                ),
                # The first 8 bytes are header, then 4 bytes are the length
                # of our element, then the encoding of our actual element.
                # We include the length so we can distinguish NULL (len=-1)
                # from empty strings and the like (len=0).
                pgast.NumericConstant(val="9"),
            ],
        )
        cast_name = s_casts.get_cast_fullname_from_names(
            sn.QualName('std', 'bytes'), sn.QualName('std', 'json'))
        val = pgast.FuncCall(
            name=common.get_cast_backend_name(cast_name, aspect='function'),
            args=[val],
        )

    if op.backend_setting and op.scope is qltypes.ConfigScope.INSTANCE:
        assert isinstance(val, pgast.SelectStmt) and len(val.target_list) == 1
        val = val.target_list[0].val
        if isinstance(val, pgast.TypeCast):
            val = val.arg
        if not isinstance(val, pgast.BaseConstant):
            raise AssertionError('value is not a constant in ConfigSet')

    return val


def top_output_as_config_op(
        ir_set: irast.Set,
        stmt: pgast.SelectStmt, *,
        env: context.Environment) -> pgast.Query:

    assert isinstance(ir_set.expr, irast.ConfigCommand)
    op = ir_set.expr

    alias = env.aliases.get('cfg')
    cte = pgast.CommonTableExpr(query=stmt, name=alias)
    ctes = [cte]

    subrvar = relctx.rvar_for_rel(cte, env=env)

    stmt_res = stmt.target_list[0]

    if stmt_res.name is None:
        stmt_res = stmt.target_list[0] = pgast.ResTarget(
            name=env.aliases.get('v'),
            val=stmt_res.val,
        )
        assert stmt_res.name is not None
    val = pgast.ColumnRef(name=[stmt_res.name])

    # FIXME: Can the duplication with other db cases be reduced?
    if op.scope is qltypes.ConfigScope.DATABASE:
        sval = pgast.SelectStmt(
            target_list=[pgast.ResTarget(val=val)], from_clause=[subrvar])
        ins_val = pgast.FuncCall(
            name=('jsonb_build_array',),
            args=[sval],
            null_safe=True,
            ser_safe=True,
        )

        old_val = pgast.CoalesceExpr(
            args=[
                pgast.ColumnRef(name=['edgedb', '_db_config', 'value']),
                pgast.TypeCast(
                    arg=pgast.StringConstant(val='[]'),
                    type_name=pgast.TypeName(
                        name=('jsonb',),
                    ),
                ),
            ],
        )
        upd_val = pgast.Expr(
            name='||',
            lexpr=old_val,
            rexpr=ins_val,
        )

        ins = pgast.InsertStmt(
            relation=pgast.RelRangeVar(
                relation=pgast.Relation(
                    name='_db_config',
                    schemaname='edgedb',
                ),
            ),
            select_stmt=pgast.SelectStmt(
                values=[
                    pgast.ImplicitRowExpr(
                        args=[
                            pgast.StringConstant(
                                val=op.name,
                            ),
                            ins_val,
                        ]
                    )
                ],
            ),
            cols=[
                pgast.InsertTarget(name='name'),
                pgast.InsertTarget(name='value'),
            ],
            on_conflict=pgast.OnConflictClause(
                action='update',
                infer=pgast.InferClause(
                    index_elems=[
                        pgast.ColumnRef(name=['name']),
                    ],
                ),
                target_list=[
                    pgast.MultiAssignRef(
                        columns=[pgast.ColumnRef(name=['value'])],
                        source=pgast.RowExpr(
                            args=[
                                upd_val,
                            ],
                        ),
                    ),
                ],
            ),
            returning_list=[
                pgast.ResTarget(
                    val=pgast.ColumnRef(name=[pgast.Star()])
                )
            ],
        )
        ctes.append(
            pgast.CommonTableExpr(query=ins, name=env.aliases.get('ins'))
        )

        subrvar = relctx.rvar_for_rel(ctes[-1], env=env)
        val = pgast.ColumnRef(name=['value'])

    if ir_set.expr.scope in (
        qltypes.ConfigScope.INSTANCE, qltypes.ConfigScope.DATABASE
    ):
        # For database config, we do SET, and we return the entire new
        # value, in order to avoid race conditions in duplicate
        # checking.
        command = (
            'SET' if ir_set.expr.scope is qltypes.ConfigScope.DATABASE
            else 'ADD'
        )
        result_row = pgast.RowExpr(
            args=[
                pgast.StringConstant(val=command),
                pgast.StringConstant(val=str(ir_set.expr.scope)),
                pgast.StringConstant(val=ir_set.expr.name),
                val,
            ]
        )

        array = pgast.FuncCall(
            name=('jsonb_build_array',),
            args=result_row.args,
            null_safe=True,
            ser_safe=True,
        )

        result = pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=array,
                ),
            ],
            from_clause=[
                subrvar,
            ],
            ctes=ctes + (stmt.ctes or []),
        )

        stmt.ctes = []

        return result
    else:
        raise errors.InternalServerError(
            f'CONFIGURE {ir_set.expr.scope} INSERT is not supported')
