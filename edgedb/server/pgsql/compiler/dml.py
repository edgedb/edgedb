##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

"""IR compiler support for INSERT/UPDATE/DELETE statements."""

#
# The processing of the DML statement is done in three parts.
#
# 1. The statement's *range* query is built: the relation representing
#    the statement's target Class with any WHERE quals taken into account.
#
# 2. The statement body is processed to generate a series of
#    SQL substatements to modify all relations touched by the statement
#    depending on the link layout.
#
# 3. The RETURNING statement is processed to and generates a SELECT statement
#    producing the result rows.  Note that the SQL's RETURNING is not used
#    on the top level, as need to be able to compute an arbitrary expression
#    in EdgeQL RETURNING clause.
#

from edgedb.lang.common import ast

from edgedb.lang.ir import ast as irast

from edgedb.lang.schema import atoms as s_atoms

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import common
from edgedb.server.pgsql import types as pg_types


class IRCompilerDMLSupport:
    def visit_InsertStmt(self, stmt):
        with self.context.substmt():
            # Common DML bootstrap
            wrapper, insert_cte, _ = \
                self._init_dml_stmt(stmt, pgast.InsertStmt())

            # Process INSERT body
            self._process_insert_body(stmt, wrapper, insert_cte)

            # Process INSERT RETURNING
            self._process_selector(stmt.result)
            self._connect_subrels(wrapper)

            return wrapper

    def visit_UpdateStmt(self, stmt):
        with self.context.substmt():
            # Common DML bootstrap
            wrapper, update_cte, range_cte = \
                self._init_dml_stmt(stmt, pgast.UpdateStmt())

            # Process UPDATE body
            self._process_update_body(stmt, wrapper, update_cte, range_cte)

            # Process UPDATE RETURNING
            self._process_selector(stmt.result)
            self._connect_subrels(wrapper)

            return wrapper

    def visit_DeleteStmt(self, stmt):
        with self.context.subquery():
            # Common DML bootstrap
            wrapper, delete_cte, _ = \
                self._init_dml_stmt(stmt, pgast.DeleteStmt())

            # Process DELETE RETURNING
            self._process_selector(stmt.result)
            self._connect_subrels(wrapper)

            return wrapper

    def _init_dml_stmt(self, ir_stmt, dml_stmt):
        """Prepare the common structure of the query representing a DML stmt.

        :param ir_stmt:
            IR of the statement.
        :param dml_stmt:
            SQL DML node instance.

        :return:
            A (*wrapper*, *dml_cte*, *range_cte*) tuple, where *wrapper* the
            the wrapping SQL statement, *dml_cte* is the CTE representing the
            SQL DML operation in the main relation of the Class, and
            *range_cte* is the CTE for the subset affected by the statement.
            *range_cte* is None for INSERT statmenets.
        """
        ctx = self.context.current

        # The wrapping query is always a SELECT to support arbitrary
        # expressions in the RETURNING clause.
        ctx.stmt = ctx.query = ctx.rel = wrapper = pgast.SelectStmt()

        if ctx.toplevel_stmt is None:
            ctx.toplevel_stmt = ctx.stmt

        toplevel = ctx.toplevel_stmt

        # Process any substatments in the WITH block.
        self._process_explicit_substmts(ir_stmt)

        target_ir_set = ir_stmt.shape.set

        dml_stmt.relation = self._range_for_concept(
            ir_stmt.shape.scls, None, include_overlays=False)
        dml_stmt.scls_rvar = dml_stmt.relation

        dml_cte = pgast.CommonTableExpr(
            query=dml_stmt,
            name=ctx.genalias(hint='m')
        )

        if isinstance(ir_stmt, (irast.UpdateStmt, irast.DeleteStmt)):
            # UPDATE and DELETE operate over a range, so generate
            # the corresponding CTE and connect it to the DML query.
            range_cte = self._get_dml_range(ir_stmt, dml_stmt)

            toplevel.ctes.append(range_cte)

            range_rvar = pgast.RangeVar(
                relation=range_cte,
                alias=pgast.Alias(
                    aliasname=ctx.genalias(hint='range')
                )
            )

            ctx.subquery_map[dml_stmt][range_cte] = {
                'linked': True,
                'rvar': range_rvar
            }

            self._pull_path_namespace(target=dml_stmt, source=range_rvar)

            # Auxillary relations are always joined via the WHERE
            # clause due to the structure of the UPDATE/DELETE SQL statments.
            id_col = common.edgedb_name_to_pg_name('std::id')
            dml_stmt.where_clause = self._new_binop(
                lexpr=pgast.ColumnRef(name=[
                    dml_stmt.relation.alias.aliasname,
                    id_col
                ]),
                op=ast.ops.EQ,
                rexpr=dml_stmt.path_namespace[target_ir_set.path_id]
            )

            # UPDATE has "FROM", while DELETE has "USING".
            if hasattr(dml_stmt, 'from_clause'):
                dml_stmt.from_clause.append(range_rvar)
            else:
                dml_stmt.using_clause.append(range_rvar)

        else:
            # No range CTE for INSERT statements, however we need
            # to make sure it RETURNs the inserted entity id, which
            # we will require when updating the link relations as
            # a result of INSERT body processing.
            range_cte = None

            target_id_set = self._get_ptr_set(target_ir_set, 'std::id')

            self._pull_path_var(
                dml_stmt, target_id_set, path_id=target_ir_set.path_id)

        # Record the effect of this insertion in the relation overlay
        # context to ensure that the RETURNING clause potentially
        # referencing this class yields the expected results.
        overlays = ctx.rel_overlays[ir_stmt.shape.scls]
        overlays.append(('union', dml_cte))

        # Finaly set the DML CTE as the source for paths originating
        # in its relation.
        toplevel.ctes.append(dml_cte)
        ctx.ctemap[ir_stmt.shape.set] = dml_cte

        return wrapper, dml_cte, range_cte

    def _get_dml_range(self, ir_stmt, dml_stmt):
        """Create a range CTE for the given DML statement.

        :param ir_stmt:
            IR of the statement.
        :param dml_stmt:
            SQL DML node instance.

        :return:
            A CommonTableExpr node representing the range affected
            by the DML statement.
        """
        target_ir_set = ir_stmt.shape.set
        ir_qual_expr = ir_stmt.where

        with self.context.subquery():
            ctx = self.context.current

            range_stmt = ctx.query

            id_set = self._get_ptr_set(target_ir_set, 'std::id')
            self.visit(id_set)

            target_cte = ctx.ctemap[target_ir_set]

            range_stmt.scls_rvar = \
                ctx.subquery_map[range_stmt][target_cte]['rvar']

            self._pull_path_var(
                range_stmt, id_set, path_id=target_ir_set.path_id)

            if ir_qual_expr is not None:
                with self.context.new():
                    self.context.current.location = 'where'
                    range_stmt.where_clause = self.visit(ir_qual_expr)

            self._connect_subrels(range_stmt)

            range_cte = pgast.CommonTableExpr(
                query=range_stmt,
                name=ctx.genalias(hint='range')
            )

            return range_cte

    def _process_insert_body(self, ir_stmt, wrapper, insert_cte):
        """Generate SQL DML CTEs from an InsertStmt IR.

        :param ir_stmt:
            IR of the statement.
        :param wrapper:
            Top-level SQL query.
        :param insert_cte:
            CTE representing the SQL INSERT to the main relation of the Class.
        """
        ctx = self.context.current

        cols = [pgast.ColumnRef(name=['std::__class__'])]
        select = pgast.SelectStmt()
        values = pgast.ImplicitRowExpr()
        select.values = [values]

        # The main INSERT query of this statement will always be
        # present to insert at least the std::id and std::__class__
        # links.
        insert_stmt = insert_cte.query

        insert_stmt.cols = cols
        insert_stmt.select_stmt = select

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
                    rexpr=pgast.Constant(val=ir_stmt.shape.scls.name)
                )
            )
        )

        external_inserts = []

        with self.context.subquery():
            # It is necessary to process the expressions in
            # the UpdateStmt shape body in the context of the
            # UPDATE statement so that references to the current
            # values of the updated object are resolved correctly.
            ctx = self.context.current
            ctx.rel = ctx.query = select
            ctx.output_format = 'flat'

            # Process the Insert IR and separate links that go
            # into the main table from links that are inserted into
            # a separate link table.
            for shape_el in ir_stmt.shape.elements:
                ptrcls = shape_el.rptr.ptrcls
                insvalue = shape_el.expr

                ptr_info = pg_types.get_pointer_storage_info(
                    ptrcls, schema=ctx.schema, resolve_type=True,
                    link_bias=False)

                props_only = False

                # First, process all local link inserts.
                if ptr_info.table_type == 'concept':
                    props_only = True
                    field = pgast.ColumnRef(name=[ptr_info.column_name])
                    cols.append(field)

                    with self.context.new():
                        insvalue = pgast.TypeCast(
                            arg=self.visit(insvalue),
                            type_name=pgast.TypeName(
                                name=ptr_info.column_type))

                        values.args.append(insvalue)

                ptr_info = pg_types.get_pointer_storage_info(
                    ptrcls, resolve_type=False, link_bias=True)

                if ptr_info and ptr_info.table_type == 'link':
                    external_inserts.append((shape_el, props_only))

            self._connect_subrels(insert_stmt)

        # Process necessary updates to the link tables.
        for expr, props_only in external_inserts:
            self._process_link_update(
                ir_stmt, expr, props_only, wrapper, insert_cte)

    def _process_update_body(self, ir_stmt, wrapper, update_cte, range_cte):
        """Generate SQL DML CTEs from an UpdateStmt IR.

        :param ir_stmt:
            IR of the statement.
        :param wrapper:
            Top-level SQL query.
        :param update_cte:
            CTE representing the SQL UPDATE to the main relation of the Class.
        :param range_cte:
            CTE representing the range affected by the statement.
        """
        update_stmt = update_cte.query

        external_updates = []

        with self.context.subquery():
            # It is necessary to process the expressions in
            # the UpdateStmt shape body in the context of the
            # UPDATE statement so that references to the current
            # values of the updated object are resolved correctly.
            ctx = self.context.current
            ctx.rel = ctx.query = update_stmt
            ctx.output_format = 'flat'
            ctx.ctemap[ir_stmt.shape.set] = range_cte

            for shape_el in ir_stmt.shape.elements:
                ptrcls = shape_el.rptr.ptrcls
                updvalue = shape_el.expr

                ptr_info = pg_types.get_pointer_storage_info(
                    ptrcls, schema=ctx.schema, resolve_type=True,
                    link_bias=False)

                props_only = False

                # First, process all internal link updates
                if ptr_info.table_type == 'concept':
                    props_only = True

                    updvalue = pgast.TypeCast(
                        arg=self.visit(updvalue),
                        type_name=pgast.TypeName(
                            name=ptr_info.column_type))

                    update_stmt.targets.append(
                        pgast.UpdateTarget(
                            name=ptr_info.column_name,
                            val=updvalue))

                ptr_info = pg_types.get_pointer_storage_info(
                    ptrcls, resolve_type=False, link_bias=True)

                if ptr_info and ptr_info.table_type == 'link':
                    external_updates.append((shape_el, props_only))

            self._connect_subrels(update_stmt)

        if not update_stmt.targets:
            # No updates directly to the set target table,
            # so convert the UPDATE statement into a SELECT.
            update_cte.query = pgast.SelectStmt(
                ctes=update_stmt.ctes,
                target_list=update_stmt.returning_list,
                from_clause=[update_stmt.relation] + update_stmt.from_clause,
                where_clause=update_stmt.where_clause,
                path_namespace=update_stmt.path_namespace,
                path_vars=update_stmt.path_vars,
                path_bonds=update_stmt.path_bonds,
                scls_rvar=update_stmt.scls_rvar
            )

        # Process necessary updates to the link tables.
        for expr, props_only in external_updates:
            self._process_link_update(
                ir_stmt, expr, props_only, wrapper, update_cte)

    def _process_link_update(self, ir_stmt, ir_expr, props_only,
                             wrapper, dml_cte):
        """Perform updates to a link relation as part of a DML statement.

        :param ir_stmt:
            IR of the statement.
        :param ir_expr:
            IR of the INSERT/UPDATE body element.
        :param props_only:
            Whether this link update only touches link properties.
        :param wrapper:
            Top-level SQL query.
        :param dml_cte:
            CTE representing the SQL UPDATE to the main relation of the Class.
        """
        ctx = self.context.current

        toplevel = ctx.toplevel_stmt

        edgedb_link = pgast.RangeVar(
            relation=pgast.Relation(
                schemaname='edgedb', relname='link'
            ),
            alias=pgast.Alias(aliasname=ctx.genalias(hint='l')))

        ltab_alias = edgedb_link.alias.aliasname

        rptr = ir_expr.rptr
        ptrcls = rptr.ptrcls
        target_is_atom = isinstance(rptr.target, s_atoms.Atom)

        # Lookup link class id by link name.
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
        toplevel.ctes.append(lname_to_id)

        target_tab = self._range_for_ptrcls(
            ptrcls, '>', include_overlays=False)
        target_alias = target_tab.alias.aliasname

        if target_is_atom:
            target_tab_name = (target_tab.schema, target_tab.name)
        else:
            target_tab_name = common.link_name_to_table_name(
                ptrcls.shortname, catenate=False)

        tab_cols = \
            ctx.backend._type_mech.get_cached_table_columns(target_tab_name)

        assert tab_cols, "could not get cols for {!r}".format(target_tab_name)

        col_data = {
            'link_type_id': pgast.ColumnRef(
                name=[
                    lname_to_id.name,
                    'id'
                ]
            ),
            'std::source': pgast.ColumnRef(
                name=[
                    dml_cte.name,
                    self._get_path_var(
                        dml_cte.query, ir_stmt.shape.set.path_id)
                ]
            )
        }

        # Drop all previous link records for this source.
        delcte = pgast.CommonTableExpr(
            query=pgast.DeleteStmt(
                relation=target_tab,
                where_clause=self._new_binop(
                    lexpr=col_data['std::source'],
                    op=ast.ops.EQ,
                    rexpr=pgast.ColumnRef(
                        name=[target_alias, 'std::source'])
                ),
                using_clause=[pgast.RangeVar(relation=dml_cte)],
                returning_list=[
                    pgast.ResTarget(
                        val=pgast.ColumnRef(
                            name=[target_alias, pgast.Star()]))
                ],
                rptr_rvar=target_tab
            ),
            name=ctx.genalias(hint='d')
        )

        # Record the effect of this removal in the relation overlay
        # context to ensure that the RETURNING clause potentially
        # referencing this link yields the expected results.
        overlays = ctx.rel_overlays[ptrcls]
        overlays.append(('except', delcte))
        toplevel.ctes.append(delcte)

        # Turn the IR of the expression on the right side of :=
        # into one or more sub-selects.
        tranches = self._process_link_values(
            ir_expr, target_tab_name, tab_cols, col_data,
            [pgast.RangeVar(relation=dml_cte), lname_to_id_rvar],
            props_only, target_is_atom)

        for cols, data_cte in tranches:
            toplevel.ctes.append(data_cte)
            data_select = pgast.SelectStmt(
                target_list=[
                    pgast.ResTarget(
                        val=pgast.ColumnRef(
                            name=[data_cte.name, pgast.Star()]))
                ],
                from_clause=[
                    pgast.RangeVar(relation=data_cte)
                ]
            )

            # Inserting rows into the link table may produce cardinality
            # constraint violations, since the INSERT into the link table
            # is executed in the snapshot where the above DELETE from
            # the link table is not visible.  Hence, we need to use
            # the ON CONFLICT clause to resolve this.
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
                            name=[data_cte.name, pgast.Star()]))
                ],
                from_clause=[
                    pgast.RangeVar(relation=data_cte)
                ],
                where_clause=self._new_binop(
                    lexpr=pgast.ImplicitRowExpr(args=conflict_inference),
                    rexpr=pgast.ImplicitRowExpr(args=conflict_exc_row),
                    op='='
                )
            )

            cols = [pgast.ColumnRef(name=[col]) for col in cols]
            updcte = pgast.CommonTableExpr(
                name=ctx.genalias(hint='i'),
                query=pgast.InsertStmt(
                    relation=target_tab,
                    rptr_rvar=target_tab,
                    select_stmt=data_select,
                    cols=cols,
                    on_conflict=pgast.OnConflictClause(
                        action='update',
                        infer=pgast.InferClause(
                            index_elems=conflict_inference
                        ),
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

            # Record the effect of this insertion in the relation overlay
            # context to ensure that the RETURNING clause potentially
            # referencing this link yields the expected results.
            overlays = ctx.rel_overlays[ptrcls]
            overlays.append(('union', updcte))

            toplevel.ctes.append(updcte)

    def _process_link_values(
            self, ir_expr, target_tab, tab_cols, col_data, sources,
            props_only, target_is_atom):
        """Unpack data from an update expression into a series of selects.

        :param ir_expr:
            IR of the INSERT/UPDATE body element.
        :param target_tab:
            The link table being updated.
        :param tab_cols:
            A sequence of columns in the table being updated.
        :param col_data:
            Expressions used to populate well-known columns of the link
            table such as std::source and std::__class__.
        :param sources:
            A list of relations which must be joined into the data query
            to resolve expressions in *col_data*.
        :param props_only:
            Whether this link update only touches link properties.
        :param target_is_atom:
            Whether the link target is an Atom.
        """
        ctx = self.context.current

        tranches = []

        data = ir_expr.expr
        props = ['std::target']

        if (props == ['std::target'] and props_only and not target_is_atom):
            # No property upates and the target value is stored
            # in the source table, so we don't need to modify
            # any link tables.
            #
            return tranches

        with self.context.new():
            self.context.current.output_format = 'flat'
            input_rel = self.visit(data)

        input_stmt = input_rel

        input_rvar = pgast.RangeSubselect(
            subquery=input_rel,
            alias=pgast.Alias(
                aliasname=ctx.genalias('val')
            )
        )

        ptrcls = ir_expr.rptr.ptrcls
        if isinstance(ptrcls, s_atoms.Atom):
            target_id_col = common.edgedb_name_to_pg_name('std::target')
        else:
            target_id_col = common.edgedb_name_to_pg_name('std::id')

        input_rel_alias = input_rvar.alias.aliasname

        unnested = pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=pgast.ColumnRef(
                        name=[input_rel_alias, pgast.Star()]))
            ],
            from_clause=[input_rvar],
            where_clause=pgast.NullTest(
                arg=pgast.ColumnRef(name=[input_rel_alias, target_id_col]),
                negated=True
            )
        )

        unnested_rvar = pgast.RangeSubselect(
            subquery=unnested,
            alias=pgast.Alias(aliasname=ctx.genalias('j'))
        )

        unnested_alias = unnested_rvar.alias.aliasname

        row = pgast.ImplicitRowExpr()

        source_data = {}
        for rt in input_stmt.target_list:
            source_data[rt.name] = expr = pgast.ColumnRef(
                name=[unnested_alias, rt.name])

        for col in tab_cols:
            if col in {'std::target@atom', 'std::target'}:
                col = 'std::target'
                source_col = target_id_col
            else:
                source_col = col

            expr = col_data.get(col)
            if expr is None:
                expr = source_data.get(source_col)

            if expr is None:
                if tab_cols[col]['column_default'] is not None:
                    expr = pgast.LiteralExpr(
                        expr=tab_cols[col]['column_default'])
                else:
                    expr = pgast.Constant(val=None)

            row.args.append(expr)

        tranch_data = pgast.CommonTableExpr(
            query=pgast.SelectStmt(
                target_list=[
                    pgast.ResTarget(
                        val=pgast.Indirection(
                            arg=pgast.TypeCast(
                                arg=row,
                                type_name=pgast.TypeName(
                                    name=target_tab
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
