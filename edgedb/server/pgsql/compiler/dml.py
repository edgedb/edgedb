##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import ast

from edgedb.lang.ir import ast2 as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import atoms as s_atoms

from edgedb.server.pgsql import ast2 as pgast
from edgedb.server.pgsql import common
from edgedb.server.pgsql import types as pg_types


class IRCompilerDMLSupport:
    def visit_InsertStmt(self, stmt):
        with self.context.subquery():
            ctx = self.context.current
            ctx.query = pgast.InsertStmt()

            if stmt.substmts:
                for substmt in stmt.substmts:
                    with self.context.subquery():
                        cte = pgast.CommonTableExpr(
                            subquery=self.visit(substmt),
                            name=substmt.name
                        )
                    ctx.query.ctes.add(cte)
                    ctx.explicit_cte_map[substmt] = cte

            ctx.query.relation = self._range_for_concept(
                stmt.shape.scls, None)

            path_id = irutils.LinearPath([stmt.shape.scls])

            refexpr = pgast.ColumnRef(
                name=[
                    ctx.query.relation.alias.aliasname,
                    common.edgedb_name_to_pg_name('std::id')
                ]
            )

            ctx.query.path_namespace[path_id] = refexpr
            ctx.entityref_as_id = True

            return self._process_insert_data(stmt)

    def visit_UpdateStmt(self, stmt):
        with self.context.subquery():
            ctx = self.context.current
            update_range = ctx.query

            with self.context.new():
                self.context.current.entityref_as_id = True
                self.context.current.output_format = None
                self._process_selector(stmt.result)

            if stmt.substmts:
                for substmt in stmt.substmts:
                    with self.context.subquery():
                        ctx.rel = ctx.query = pgast.CTE()
                        cte = self.visit(substmt)
                        cte.alias = substmt.name
                    ctx.query.ctes.add(cte)
                    ctx.explicit_cte_map[substmt] = cte

            if stmt.where:
                with self.context.new():
                    self.context.current.location = 'where'
                    where = self.visit(stmt.where)

                ctx.query.where_clause = where

            self._connect_subrels(ctx.query)

            path_id = irutils.LinearPath([stmt.shape.scls])
            update_range.returning_list = [
                update_range.path_namespace[path_id]
            ]

            update_target = self._range_for_concept(
                stmt.shape.scls, None)

            id_ref = pgast.ColumnRef(
                name=[update_target.alias.aliasname, 'std::id'])

            ctx.query = pgast.UpdateStmt(
                relation=update_target,
                where_clause=self._new_binop(
                    lexpr=id_ref, op='IN', rexpr=update_range)
            )

            ctx.entityref_as_id = True

            return self._process_update_data(stmt)

    def visit_DeleteStmt(self, stmt):
        with self.context.subquery():
            ctx = self.context.current
            delete_range = ctx.query

            with self.context.new():
                self._process_selector(stmt.result)

            if stmt.substmts:
                for substmt in stmt.substmts:
                    with self.context.subquery():
                        ctx.rel = ctx.query = pgast.CTE()
                        cte = self.visit(substmt)
                        cte.alias = substmt.name
                    ctx.query.ctes.add(cte)
                    ctx.explicit_cte_map[substmt] = cte

            if stmt.where:
                with self.context.new():
                    self.context.current.location = 'where'
                    where = self.visit(stmt.where)

                ctx.query.where_clause = where

            self._connect_subrels(ctx.query)

            path_id = irutils.LinearPath([stmt.shape.scls])

            idalias = ctx.genalias('id')
            ctx.query.target_list.append([
                pgast.ResTarget(
                    val=ctx.query.path_namespace[path_id],
                    name=idalias
                )
            ])

            return_alias = ctx.query.target_list[0].name
            if not return_alias:
                return_alias = ctx.genalias('v')
                ctx.query.target_list[0].name = return_alias

            delete_target = self._range_for_concept(
                stmt.shape.scls, None)

            id_ref = pgast.ColumnRef(
                name=[delete_target.alias.aliasname, 'std::id'])

            del_range_cte = pgast.CommonTableExpr(
                query=delete_range,
                name=ctx.genalias('d')
            )

            del_range_alias = ctx.genalias('q')
            del_range_rvar = pgast.RangeVar(
                relation=del_range_cte,
                alias=pgast.Alias(aliasname=del_range_alias)
            )

            ctx.query = pgast.DeleteStmt(
                ctes=[del_range_cte],
                relation=delete_target,
                using_clause=[del_range_rvar],
                where_clause=self._new_binop(
                    lexpr=id_ref,
                    op='=',
                    rexpr=pgast.ColumnRef(name=[del_range_alias, idalias])
                )
            )

            ctx.query.returning_list.append(
                pgast.ResTarget(val=pgast.ColumnRef(
                    name=[del_range_alias, return_alias]
                ))
            )

            return ctx.query

    def _process_insert_data(self, stmt):
        """Generate SQL INSERTs from an Insert IR."""
        ctx = self.context.current

        cols = [pgast.ColumnRef(name=['std::__class__'])]
        select = pgast.SelectStmt()
        values = pgast.ImplicitRowExpr()
        select.values = [values]

        query = ctx.query

        query.cols = cols
        query.select_stmt = select

        # Type reference is always inserted.
        values.args.append(
            pgast.SelectStmt(
                target_list=[
                    pgast.ResTarget(
                        val=pgast.ColumnRef(name=['id']))
                ],
                from_clause=[
                    pgast.RangeVar(relation=pgast.Relation(
                        relname='concept', schemaname='edgedb'))
                ],
                where_clause=self._new_binop(
                    op=ast.ops.EQ,
                    lexpr=pgast.ColumnRef(name=['name']),
                    rexpr=pgast.Constant(val=stmt.shape.scls.name)
                )
            )
        )

        if not stmt.shape.elements:
            return query

        external_inserts = []

        for expr in stmt.shape.elements:
            ptrcls = expr.rptr.ptrcls
            insvalue = expr.stmt.result

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, schema=ctx.schema, resolve_type=True,
                link_bias=False)

            props_only = False
            ins_props = None
            operation = None

            # First, process all local link inserts.
            if ptr_info.table_type == 'concept':
                props_only = True
                field = pgast.ColumnRef(name=[ptr_info.column_name])
                cols.append(field)

                with self.context.new():
                    if self._is_composite_cast(insvalue):
                        insvalue, ins_props = self._extract_update_value(
                            insvalue, ptr_info.column_type)

                    else:
                        insvalue = pgast.TypeCast(
                            arg=self.visit(insvalue),
                            type_name=pgast.TypeName(
                                name=ptr_info.column_type))

                    values.args.append(insvalue)

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, resolve_type=False, link_bias=True)

            if ptr_info and ptr_info.table_type == 'link':
                external_inserts.append((expr, props_only, operation))

        toplevel = None

        # Inserting externally-stored links requires repackaging everything
        # into a series of CTEs so that multiple statements can be executed
        # as a single query.
        #
        for expr, props_only, operation in external_inserts:
            if toplevel is None:
                toplevel = pgast.SelectStmt()
                query_cte = pgast.CommonTableExpr(
                    query=query,
                    name=ctx.genalias(hint='m')
                )
                toplevel.ctes.append(query_cte)

                query.returning_list.append(
                    pgast.ResTarget(
                        val=pgast.ColumnRef(name=['std::id'])))

                ref = pgast.ColumnRef(
                    name=[query_cte.name, 'std::id'])
                toplevel.from_clause.append(
                    pgast.RangeVar(relation=query_cte))

                if ctx.output_format == 'json':
                    keyvals = [pgast.Constant(val='id'), ref]
                    target = pgast.ResTarget(val=pgast.FuncCall(
                        name='jsonb_build_object', args=keyvals))
                else:
                    target = pgast.ResTarget(val=ref)

                toplevel.target_list.append(target)

            self._process_update_expr(
                expr, props_only, operation, toplevel, query_cte)

        if toplevel is not None:
            query = toplevel
        else:
            ref = pgast.ColumnRef(
                name=[query.relation.alias.aliasname, 'std::id'])
            if ctx.output_format == 'json':
                keyvals = [pgast.Constant(val='id'), ref]
                target = pgast.ResTarget(val=pgast.FuncCall(
                    name='jsonb_build_object', args=keyvals))
            else:
                target = pgast.ResTarget(val=ref)
            query.returning_list.append(target)

        return query

    def _process_update_values(
            self, updvalexpr, target_tab, tab_cols, col_data, sources,
            props_only, target_is_atom):
        """Unpack data from an update expression into a series of selects."""
        ctx = self.context.current

        # Recurse down to process update expressions like
        # col := col + val1 + val2
        #
        if (isinstance(updvalexpr, irast.BinOp) and
                isinstance(updvalexpr.left, irast.BinOp)):
            tranches = self._process_update_values(
                updvalexpr.left, target_tab, tab_cols, col_data,
                sources, props_only, target_is_atom)
        else:
            tranches = []

        if isinstance(updvalexpr, irast.BinOp):
            updval = updvalexpr.right
        else:
            updval = updvalexpr

        if isinstance(updval, irast.TypeCast):
            # Link property updates will have the data casted into
            # an appropriate selector shape which specifies which properties
            # are being updated.
            #
            data = updval.expr
            typ = updval.type

            if not isinstance(typ, tuple):
                raise ValueError(
                    'unexpected value type in update expr: {!r}'.format(typ))

            if not isinstance(typ[1], irast.CompositeType):
                raise ValueError(
                    'unexpected value type in update expr: {!r}'.format(typ))

            props = [p.ptr_class.normal_name() for p in typ[1].pathspec]

        else:
            # Target-only update
            #
            data = updval
            props = ['std::target']

        e = common.edgedb_name_to_pg_name

        spec_cols = {e(prop): i for i, prop in enumerate(props)}

        if (props == ['std::target'] and props_only and not target_is_atom):
            # No property upates and the target value is stored
            # in the source table, so we don't need to modify
            # any link tables.
            #
            return tranches

        with self.context.new():
            self.context.current.output_format = None
            input_data = self.visit(data)

        if (isinstance(input_data, pgast.Constant) and
                input_data.type.endswith('[]')):
            data_is_json = input_data.type == 'json[]'
            input_data = pgast.FuncCall(
                name='UNNEST', args=[input_data])
        else:
            data_is_json = False

        input_rel = pgast.RangeSubselect(
            subquery=input_data,
            alias=pgast.Alias(aliasname=ctx.genalias('i'))
        )

        input_rel_alias = input_rel.alias.aliasname

        unnested = pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=pgast.ColumnRef(
                        name=[input_rel_alias, pgast.Star()]))
            ],
            from_clause=[input_rel])

        unnested_rvar = pgast.RangeSubselect(
            subquery=unnested,
            alias=pgast.Alias(
                aliasname=ctx.genalias('j'),
                colnames=[
                    pgast.ColumnDef(
                        name='_'
                    )
                ])
        )

        unnested_alias = unnested_rvar.alias.aliasname

        row = pgast.ImplicitRowExpr()

        for col in tab_cols:
            if (col == 'std::target' and (props_only or target_is_atom)):
                expr = pgast.TypeCast(
                    arg=pgast.Constant(val=None),
                    type_name=pgast.TypeName(name='uuid'))
            else:
                if col == 'std::target@atom':
                    col = 'std::target'

                data_idx = spec_cols.get(col)
                if data_idx is None:
                    try:
                        expr = col_data[col]
                    except KeyError:
                        if tab_cols[col]['column_default'] is not None:
                            expr = pgast.LiteralExpr(
                                expr=tab_cols[col]['column_default'])
                        else:
                            expr = pgast.Constant(val=None)
                else:
                    expr = pgast.ColumnRef(
                        name=[unnested_alias, '_'])
                    if data_is_json:
                        expr = self._new_binop(
                            lexpr=expr, op='->>',
                            rexpr=pgast.Constant(val=data_idx))

            row.args.append(expr)

        tranch_data = pgast.CommonTableExpr(
            query=pgast.SelectStmt(
                target_list=[
                    pgast.ResTarget(
                        val=pgast.Indirection(
                            arg=pgast.TypeCast(
                                arg=row,
                                type_name=pgast.TypeName(
                                    name=common.qname(*target_tab)
                                )
                            ),
                            indirection=[pgast.Star()]
                        )
                    )
                ],
                from_clause=[unnested_rvar],
            ),
            name=ctx.genalias(hint='r')
        )

        tranch_data.query.from_clause.extend(sources)

        tranches.append((tab_cols, tranch_data))

        return tranches

    def _process_update_expr(self, updexpr, props_only, operation, query,
                             scope_cte):
        ctx = self.context.current

        edgedb_link = pgast.RangeVar(
            relation=pgast.Relation(
                schemaname='edgedb', relname='link'
            ),
            alias=pgast.Alias(aliasname=ctx.genalias(hint='l')))

        ltab_alias = edgedb_link.alias.aliasname

        rptr = updexpr.rptr
        ptrcls = rptr.ptrcls
        target_is_atom = isinstance(rptr.target, s_atoms.Atom)

        lname_to_id = pgast.CommonTableExpr(
            query=pgast.SelectStmt(
                from_clause=[
                    edgedb_link
                ],
                target_list=[
                    pgast.ResTarget(
                        val=pgast.ColumnRef(name=[ltab_alias, 'id']))
                ],
                where_clause=self._new_binop(
                    lexpr=pgast.ColumnRef(name=[ltab_alias, 'name']),
                    rexpr=pgast.Constant(val=ptrcls.name),
                    op=ast.ops.EQ
                )
            ),
            name=ctx.genalias(hint='lid')
        )

        lname_to_id_rvar = pgast.RangeVar(relation=lname_to_id)

        query.ctes.append(lname_to_id)

        target_tab = self._range_for_ptrcls(ptrcls, '>')
        target_alias = target_tab.alias.aliasname

        if target_is_atom:
            target_tab_name = (target_tab.schema, target_tab.name)
        else:
            target_tab_name = common.link_name_to_table_name(
                ptrcls.normal_name(), catenate=False)

        tab_cols = \
            ctx.backend._type_mech.get_cached_table_columns(target_tab_name)

        assert tab_cols, "could not get cols for {!r}".format(target_tab_name)

        col_data = {
            'link_type_id': pgast.ColumnRef(
                name=[lname_to_id.name, 'id']),
            'std::source': pgast.ColumnRef(
                name=[scope_cte.name, 'std::id'])
        }

        if operation is None:
            # Drop previous entries first
            delcte = pgast.CommonTableExpr(
                query=pgast.DeleteStmt(
                    relation=target_tab,
                    where_clause=self._new_binop(
                        lexpr=col_data['std::source'],
                        op=ast.ops.EQ,
                        rexpr=pgast.ColumnRef(
                            name=[target_alias, 'std::source'])
                    ),
                    using_clause=[pgast.RangeVar(relation=scope_cte)],
                    returning_list=[
                        pgast.ResTarget(
                            val=col_data['std::source'],
                            name='std::id')
                    ]
                ),
                name=ctx.genalias(hint='d')
            )
            query.ctes.append(delcte)
            scope_cte = pgast.JoinExpr(
                type='NATURAL LEFT',
                larg=pgast.RangeVar(relation=scope_cte),
                rarg=pgast.RangeVar(relation=delcte))
        else:
            delcte = None

        tranches = self._process_update_values(
            updexpr, target_tab_name, tab_cols, col_data,
            [scope_cte, lname_to_id_rvar], props_only, target_is_atom)

        for cols, data in tranches:
            query.ctes.append(data)
            data = pgast.SelectStmt(
                target_list=[
                    pgast.ResTarget(
                        val=pgast.ColumnRef(name=[data.name, pgast.Star()]))
                ],
                from_clause=[
                    pgast.RangeVar(relation=data)
                ]
            )

            if operation == ast.ops.SUB:
                # Removing links
                updcte = pgast.CommonTableExpr(
                    query=pgast.DeleteStmt(
                        returning_list=[
                            pgast.ResTarget(
                                val=pgast.ColumnRef(name=['std::source']),
                                name='std::id')
                        ]
                    ),
                    name=ctx.genalias(hint='d'),
                )

                updcte.query.relation = target_tab

                updcte.where = self._new_binop(
                    lexpr=pgast.ColumnRef(name=['std::linkid']),
                    op=ast.ops.IN,
                    rexpr=pgast.SelectStmt(
                        target_list=[
                            pgast.ResTarget(
                                val=pgast.ColumnRef(
                                    name=['std::linkid']))
                        ],
                        from_clause=[
                            pgast.RangeSubselect(
                                subquery=data,
                                alias=pgast.Alias(
                                    aliasname=ctx.genalias(hint='q')
                                )
                            )
                        ]
                    )
                )

            else:
                # Inserting links
                updcte = pgast.CommonTableExpr(
                    name=ctx.genalias(hint='i'),
                    query=pgast.InsertStmt(
                        returning_list=[
                            pgast.ResTarget(
                                val=pgast.ColumnRef(name=['std::source']),
                                name='std::id'
                            )
                        ]
                    )
                )

                updcte.query.relation = target_tab

                updcte.query.select_stmt = data
                updcte.query.cols = [
                    pgast.ColumnRef(name=[col]) for col in cols
                ]

                update_target = pgast.MultiAssignRef(
                    source=data,
                    columns=updcte.query.cols
                )

                updcte.query.on_conflict = pgast.OnConflictClause(
                    action='update',
                    infer=[pgast.ColumnRef(name=['std::linkid'])],
                    target_list=[update_target])

            query.ctes.append(updcte)

    def _process_update_data(self, stmt):
        ctx = self.context.current
        query = ctx.query

        external_updates = []

        for expr in stmt.shape.elements:
            ptrcls = expr.rptr.ptrcls
            updvalue = expr.stmt.result

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, schema=ctx.schema, resolve_type=True, link_bias=False)

            props_only = False
            upd_props = None
            operation = None

            # First, process all internal link updates
            if ptr_info.table_type == 'concept':
                props_only = True

                with self.context.new():
                    if self._is_composite_cast(updvalue):
                        updvalue, upd_props = self._extract_update_value(
                            updvalue, ptr_info.column_type)

                    else:
                        updvalue = pgast.TypeCast(
                            arg=self.visit(updvalue),
                            type_name=pgast.TypeName(
                                name=ptr_info.column_type))

                    query.targets.append(
                        pgast.UpdateTarget(
                            name=ptr_info.column_name,
                            val=updvalue))

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, resolve_type=False, link_bias=True)

            if ptr_info and ptr_info.table_type == 'link':
                external_updates.append((expr, props_only, operation))

        if not query.targets:
            # No atomic updates

            query.returning_list.append(
                pgast.ResTarget(
                    val=pgast.ColumnRef(name=['std::id'])))

            query = pgast.SelectStmt(
                ctes=query.ctes,
                target_list=query.returning_list,
                from_clause=[query.relation],
                where_clause=query.where_clause
            )

        if not external_updates:
            ref = pgast.ColumnRef(name=['std::id'])

            if ctx.output_format == 'json':
                keyvals = [pgast.Constant(val='id'), ref]
                target = pgast.ResTarget(val=pgast.FuncCall(
                    name='jsonb_build_object', args=keyvals))
            else:
                target = pgast.ResTarget(val=ref)

            query.returning_list.append(target)

        toplevel = None

        # Updating externally-stored linksrequires repackaging everything into
        # a series of CTEs so that multiple statements can be executed as a
        # single query.
        #
        for expr, props_only, operation in external_updates:
            if toplevel is None:
                toplevel = pgast.SelectStmt()
                query_cte = pgast.CommonTableExpr(
                    query=query,
                    name=ctx.genalias(hint='m')
                )
                toplevel.ctes.append(query_cte)

                ref = pgast.ColumnRef(
                    name=[query_cte.name, 'std::id'])
                toplevel.from_clause.append(
                    pgast.RangeVar(relation=query_cte))

                if ctx.output_format == 'json':
                    keyvals = [pgast.Constant(val='id'), ref]
                    target = pgast.ResTarget(val=pgast.FuncCall(
                        name='jsonb_build_object', args=keyvals))
                else:
                    target = pgast.ResTarget(val=ref)

                toplevel.target_list.append(target)

            self._process_update_expr(
                expr, props_only, operation, toplevel, query_cte)

        if toplevel is not None:
            query = toplevel

        return query
