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
            ctx.rel = ctx.query = pgast.InsertStmt()

            toplevel, insert_cte = self._init_dml_stmt(stmt)

            with self.context.new():
                self.context.current.entityref_as_id = True
                self._process_insert_data(stmt, toplevel, insert_cte)

            with self.context.new():
                ctx = self.context.current
                ctx.rel = ctx.query = toplevel

                self._process_selector(stmt.result)
                self._connect_subrels(toplevel)

            return toplevel

    def visit_UpdateStmt(self, stmt):
        with self.context.subquery():
            ctx = self.context.current
            ctx.rel = ctx.query = pgast.UpdateStmt()

            toplevel, update_cte = self._init_dml_stmt(stmt)

            with self.context.new():
                self.context.current.entityref_as_id = True
                self._process_update_data(
                    stmt, toplevel, update_cte)

            with self.context.new():
                ctx = self.context.current
                ctx.rel = ctx.query = toplevel

                self._process_selector(stmt.result)
                self._connect_subrels(toplevel)

            return toplevel

    def visit_DeleteStmt(self, stmt):
        with self.context.subquery():
            ctx = self.context.current
            ctx.rel = ctx.query = pgast.DeleteStmt()

            toplevel, delete_cte = self._init_dml_stmt(stmt)

            if stmt.where:
                self._process_dml_quals(
                    stmt.shape.set, stmt.where, delete_cte.query)

            with self.context.new():
                ctx = self.context.current
                ctx.rel = ctx.query = toplevel

                self._process_selector(stmt.result)
                self._connect_subrels(toplevel)

            return toplevel

    def _init_dml_stmt(self, ir_stmt):
        ctx = self.context.current
        query = ctx.query

        self._process_explicit_substmts(ir_stmt)

        query.relation = self._range_for_concept(ir_stmt.shape.scls, None)
        query.scls_rvar = query.relation

        id_col = common.edgedb_name_to_pg_name('std::id')

        refexpr = pgast.ColumnRef(
            name=[query.relation.alias.aliasname, id_col])

        path_id = irutils.LinearPath([ir_stmt.shape.scls])
        query.path_namespace[path_id] = refexpr
        query.path_vars[path_id] = query.path_bonds[path_id] = id_col
        query.returning_list.append(
            pgast.ResTarget(val=pgast.ColumnRef(name=[id_col])))

        query_cte = pgast.CommonTableExpr(
            query=query,
            name=ctx.genalias(hint='m')
        )

        query_cte_rvar = pgast.RangeVar(
            relation=query_cte,
            alias=pgast.Alias(aliasname=ctx.genalias(hint='i'))
        )

        toplevel = pgast.SelectStmt(
            ctes=[query_cte],
            from_clause=[query_cte_rvar]
        )

        ctx.subquery_map[toplevel][query_cte] = {
            'linked': True,
            'rvar': query_cte_rvar
        }

        self._pull_path_namespace(
            target=toplevel, source=query_cte_rvar,
            add_to_selector=False)

        ctx.ctemap[ir_stmt.shape.set] = query_cte

        return toplevel, query_cte

    def _process_dml_quals(self, target_ir_set, ir_qual_expr, dml_stmt):
        with self.context.subquery():
            ctx = self.context.current
            ctx.ctemap.clear()

            range_stmt = ctx.query

            range_stmt.target_list = [
                pgast.ResTarget(
                    val=self.visit(
                        self._get_ptr_set(target_ir_set, 'std::id')
                    )
                )
            ]

            with self.context.new():
                self.context.current.location = 'where'
                range_stmt.where_clause = self.visit(ir_qual_expr)

            target_rvar = dml_stmt.relation

            id_ref = pgast.ColumnRef(
                name=[target_rvar.alias.aliasname, 'std::id'])

            dml_stmt.where_clause = self._new_binop(
                lexpr=id_ref, op='IN', rexpr=range_stmt)

            self._connect_subrels(range_stmt)

    def _process_insert_data(self, stmt, toplevel, insert_cte):
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

        # Inserting externally-stored links requires repackaging everything
        # into a series of CTEs so that multiple statements can be executed
        # as a single query.
        #
        for expr, props_only, operation in external_inserts:
            self._process_update_expr(
                expr, props_only, operation, toplevel, insert_cte)

    def _process_update_data(self, stmt, toplevel, update_cte):
        ctx = self.context.current
        query = ctx.query

        external_updates = []

        for expr in stmt.shape.elements:
            ptrcls = expr.rptr.ptrcls
            updvalue = expr.stmt.result

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, schema=ctx.schema, resolve_type=True,
                link_bias=False)

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

        if stmt.where:
            self._process_dml_quals(
                stmt.shape.set, stmt.where, update_cte.query)

        if not query.targets:
            # No updates directly to the set target table,
            # so convert the UPDATE statement into a SELECT.
            update_stmt = update_cte.query
            update_cte.query = pgast.SelectStmt(
                ctes=update_stmt.ctes,
                target_list=update_stmt.returning_list,
                from_clause=[update_stmt.relation],
                where_clause=update_stmt.where_clause,
                path_namespace=update_stmt.path_namespace,
                path_vars=update_stmt.path_vars,
                path_bonds=update_stmt.path_bonds,
                scls_rvar=update_stmt.scls_rvar
            )

        for expr, props_only, operation in external_updates:
            self._process_update_expr(
                expr, props_only, operation, toplevel, update_cte)

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
                            val=pgast.ColumnRef(
                                name=[target_alias, pgast.Star()]))
                    ]
                ),
                name=ctx.genalias(hint='d')
            )
            overlays = ctx.rel_overlays[ptrcls]
            overlays.append(('except', delcte))
            query.ctes.append(delcte)
        else:
            delcte = None

        tranches = self._process_update_values(
            updexpr, target_tab_name, tab_cols, col_data,
            [pgast.RangeVar(relation=scope_cte), lname_to_id_rvar],
            props_only, target_is_atom)

        for cols, data in tranches:
            query.ctes.append(data)
            data_select = pgast.SelectStmt(
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
                                subquery=data_select,
                                alias=pgast.Alias(
                                    aliasname=ctx.genalias(hint='q')
                                )
                            )
                        ]
                    )
                )

            else:
                # Inserting links

                cols = [pgast.ColumnRef(name=[col]) for col in cols]

                conflict_cols = ['std::source', 'std::target', 'link_type_id']
                conflict_inference = []
                conflict_exc_row = []

                for col in conflict_cols:
                    conflict_inference.append(
                        pgast.ColumnRef(name=[col])
                    )
                    conflict_exc_row.append(
                        pgast.ColumnRef(name=['excluded', col])
                    )

                conflict_data = pgast.SelectStmt(
                    target_list=[
                        pgast.ResTarget(
                            val=pgast.ColumnRef(
                                name=[data.name, pgast.Star()]))
                    ],
                    from_clause=[
                        pgast.RangeVar(relation=data)
                    ],
                    where_clause=self._new_binop(
                        lexpr=pgast.ImplicitRowExpr(
                            args=conflict_inference),
                        rexpr=pgast.ImplicitRowExpr(
                            args=conflict_exc_row
                        ),
                        op='='
                    )
                )

                updcte = pgast.CommonTableExpr(
                    name=ctx.genalias(hint='i'),
                    query=pgast.InsertStmt(
                        relation=target_tab,
                        select_stmt=data_select,
                        cols=cols,
                        on_conflict=pgast.OnConflictClause(
                            action='update',
                            infer=conflict_inference,
                            target_list=[
                                pgast.MultiAssignRef(
                                    columns=cols,
                                    source=conflict_data
                                )
                            ]
                        ),
                        returning_list=[
                            pgast.ResTarget(
                                val=pgast.ColumnRef(name=[pgast.Star()])
                            )
                        ]
                    )
                )

                overlays = ctx.rel_overlays[ptrcls]
                overlays.append(('union', updcte))

            query.ctes.append(updcte)

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
            if isinstance(input_data, pgast.SelectStmt):
                input_data.target_list[0] = pgast.TypeCast(
                    arg=input_data.target_list[0],
                    type_name=pgast.TypeName(name='uuid')
                )
            else:
                input_data = pgast.TypeCast(
                    arg=input_data,
                    type_name=pgast.TypeName(name='uuid')
                )

        input_rel = pgast.RangeSubselect(
            subquery=input_data,
            alias=pgast.Alias(
                aliasname=ctx.genalias('val'),
                colnames=[
                    pgast.ColumnDef(
                        name='_'
                    )
                ]
            )
        )

        input_rel_alias = input_rel.alias.aliasname

        unnested = pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=pgast.ColumnRef(
                        name=[input_rel_alias, '_']))
            ],
            from_clause=[input_rel],
            where_clause=pgast.NullTest(
                arg=pgast.ColumnRef(name=[input_rel_alias, '_']),
                negated=True
            )
        )

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
